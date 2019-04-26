/*
 * Copyright (c) 2018 - Arcade Analytics LTD (https://arcadeanalytics.com)
 */

package com.arcadedb.database;

import com.arcadedb.engine.Bucket;
import com.arcadedb.exception.DuplicatedKeyException;
import com.arcadedb.exception.RecordNotFoundException;
import com.arcadedb.index.Index;
import com.arcadedb.index.IndexCursor;
import com.arcadedb.index.TypeIndex;
import com.arcadedb.log.LogManager;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.Schema;

import java.util.*;
import java.util.logging.Level;

public class TransactionIndexContext {
  private final DatabaseInternal                               database;
  private       Map<String, Map<ComparableKey, Set<IndexKey>>> indexEntries = new LinkedHashMap<>(); // MOST COMMON USE CASE INSERTION IS ORDERED, USE AN ORDERED MAP TO OPTIMIZE THE INDEX

  public static class IndexKey {
    public final boolean  addOperation;
    public final Object[] keyValues;
    public final RID      rid;

    public IndexKey(final boolean addOperation, final Object[] keyValues, final RID rid) {
      this.addOperation = addOperation;
      this.keyValues = keyValues;
      this.rid = rid;
    }

    @Override
    public String toString() {
      return "IndexKey(" + (addOperation ? "add " : "remove ") + Arrays.toString(keyValues) + ")";
    }
  }

  public static class ComparableKey {
    public final Object[] values;

    public ComparableKey(final Object[] values) {
      this.values = values;
    }

    @Override
    public boolean equals(final Object o) {
      if (this == o)
        return true;
      if (o == null || getClass() != o.getClass())
        return false;
      final ComparableKey that = (ComparableKey) o;
      return Arrays.equals(values, that.values);
    }

    @Override
    public int hashCode() {
      return Arrays.hashCode(values);
    }
  }

  public TransactionIndexContext(final DatabaseInternal database) {
    this.database = database;
  }

  public void removeIndex(final String indexName) {
    indexEntries.remove(indexName);
  }

  public int getTotalEntries() {
    int total = 0;
    for (Map<ComparableKey, Set<IndexKey>> entry : indexEntries.values()) {
      total += entry.values().size();
    }
    return total;
  }

  public void commit() {
    checkUniqueIndexKeys();

    for (Map.Entry<String, Map<ComparableKey, Set<IndexKey>>> entry : indexEntries.entrySet()) {
      final Index index = database.getSchema().getIndexByName(entry.getKey());
      final Map<ComparableKey, Set<IndexKey>> keys = entry.getValue();

      for (Map.Entry<ComparableKey, Set<IndexKey>> keyValueEntries : keys.entrySet()) {
        final Set<IndexKey> values = keyValueEntries.getValue();

        if (values.size() > 1) {
          // BATCH MODE
          final List<RID> rids2Insert = new ArrayList<>(values.size());

          for (IndexKey key : values) {
            if (key.addOperation)
              rids2Insert.add(key.rid);
            else
              index.remove(key.keyValues, key.rid);
          }

          if (!rids2Insert.isEmpty()) {
            final RID[] rids = new RID[rids2Insert.size()];
            rids2Insert.toArray(rids);
            index.put(keyValueEntries.getKey().values, rids);
          }

        } else {
          for (IndexKey key : values) {
            if (key.addOperation)
              index.put(key.keyValues, new RID[] { key.rid });
            else
              index.remove(key.keyValues, key.rid);
          }
        }
      }
    }

    indexEntries.clear();
  }

  public void addFilesToLock(Set<Integer> modifiedFiles) {
    final Schema schema = database.getSchema();

    final Set<Index> lockedIndexes = new HashSet<>();

    for (String indexName : indexEntries.keySet()) {
      final Index index = schema.getIndexByName(indexName);

      if (lockedIndexes.contains(index))
        continue;

      lockedIndexes.add(index);

      modifiedFiles.add(index.getFileId());

      if (index.isUnique()) {
        // LOCK ALL THE FILES IMPACTED BY THE INDEX KEYS TO CHECK FOR UNIQUE CONSTRAINT
        final DocumentType type = schema.getType(index.getTypeName());
        final List<Bucket> buckets = type.getBuckets(false);
        for (Bucket b : buckets)
          modifiedFiles.add(b.getId());

        for (TypeIndex typeIndex : type.getAllIndexes())
          for (Index idx : typeIndex.getIndexesOnBuckets())
            modifiedFiles.add(idx.getFileId());
      } else
        modifiedFiles.add(index.getAssociatedBucketId());
    }
  }

  public Map<String, Map<ComparableKey, Set<IndexKey>>> toMap() {
    return indexEntries;
  }

  public void setKeys(final Map<String, Map<ComparableKey, Set<IndexKey>>> keysTx) {
    indexEntries = keysTx;
  }

  public boolean isEmpty() {
    return indexEntries.isEmpty();
  }

  public void addIndexKeyLock(final String indexName, final boolean addOperation, final Object[] keysValues, final RID rid) {
    Map<ComparableKey, Set<IndexKey>> keys = indexEntries.get(indexName);

    final ComparableKey k = new ComparableKey(keysValues);
    final IndexKey v = new IndexKey(addOperation, keysValues, rid);

    Set<IndexKey> values;
    if (keys == null) {
      keys = new LinkedHashMap<>(); // ORDERD TO KEEP INSERTION ORDER
      indexEntries.put(indexName, keys);

      values = new HashSet<>();
      keys.put(k, values);
    } else {
      values = keys.get(k);
      if (values == null) {
        values = new HashSet<>();
        keys.put(k, values);
      }
    }

    values.add(v);
  }

  public void reset() {
    indexEntries.clear();
  }

  public Map<ComparableKey, Set<IndexKey>> getIndexKeys(final String indexName) {
    return indexEntries.get(indexName);
  }

  /**
   * Called at commit time in the middle of the lock to avoid concurrent insertion of the same key.
   */
  private void checkUniqueIndexKeys(final Index index, final IndexKey key) {
    if (!key.addOperation)
      return;

    final DocumentType type = database.getSchema().getType(index.getTypeName());

    // CHECK UNIQUENESS ACROSS ALL THE INDEXES FOR ALL THE BUCKETS
    final TypeIndex idx = type.getIndexByProperties(index.getPropertyNames());
    if (idx != null) {
      final IndexCursor found = idx.get(key.keyValues, 2);

      if (found.size() > 1 || (found.size() == 1 && !found.next().equals(key.rid))) {
        try {
          database.lookupByRID(found.getRecord().getIdentity(), true);
          // NO EXCEPTION = FOUND
          throw new DuplicatedKeyException(idx.getName(), Arrays.toString(key.keyValues), found.getRecord().getIdentity());

        } catch (RecordNotFoundException e) {
          // INDEX DIRTY, THE RECORD WA DELETED, REMOVE THE ENTRY IN THE INDEX TO FIX IT
          LogManager.instance()
              .log(this, Level.WARNING, "Found entry in index '%s' with key %s pointing to the deleted record %s. Overriding it.", null, idx.getName(),
                  Arrays.toString(key.keyValues), found.getRecord().getIdentity());

          idx.remove(key.keyValues);
        }
      }
    }

  }

  private void checkUniqueIndexKeys() {
    for (Map.Entry<String, Map<ComparableKey, Set<IndexKey>>> indexEntry : indexEntries.entrySet()) {
      final Index index = database.getSchema().getIndexByName(indexEntry.getKey());
      if (index.isUnique()) {
        final Map<ComparableKey, Set<IndexKey>> entries = indexEntry.getValue();
        for (Set<IndexKey> keys : entries.values())
          for (IndexKey entry : keys)
            checkUniqueIndexKeys(index, entry);
      }
    }
  }
}
