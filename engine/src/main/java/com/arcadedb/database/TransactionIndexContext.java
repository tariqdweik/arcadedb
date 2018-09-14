/*
 * Copyright (c) 2018 - Arcade Analytics LTD (https://arcadeanalytics.com)
 */

package com.arcadedb.database;

import com.arcadedb.engine.Bucket;
import com.arcadedb.exception.DuplicatedKeyException;
import com.arcadedb.index.Index;
import com.arcadedb.index.IndexCursor;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.Schema;

import java.util.*;

public class TransactionIndexContext {
  private final DatabaseInternal                               database;
  private       Map<String, Map<ComparableKey, Set<IndexKey>>> indexEntries = new HashMap<>();

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

  public void commit() {
    checkUniqueIndexKeys();

    for (Map.Entry<String, Map<ComparableKey, Set<IndexKey>>> entry : indexEntries.entrySet()) {
      final Index index = database.getSchema().getIndexByName(entry.getKey());
      final Map<ComparableKey, Set<IndexKey>> keys = entry.getValue();

      for (Set<IndexKey> values : keys.values())
        for (IndexKey key : values) {
          if (key.addOperation)
            index.put(key.keyValues, key.rid);
          else
            index.remove(key.keyValues, key.rid);
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

        for (List<DocumentType.IndexMetadata> idxMetadatas : type.getAllIndexesMetadata()) {
          for (DocumentType.IndexMetadata idxMetadata : idxMetadatas) {
            modifiedFiles.add(idxMetadata.index.getFileId());
          }
        }
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
      keys = new HashMap<>();
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
    final List<DocumentType.IndexMetadata> typeIndexes = type.getIndexMetadataByProperties(index.getPropertyNames());
    if (typeIndexes != null) {
      for (DocumentType.IndexMetadata i : typeIndexes) {
        final IndexCursor found = i.index.get(key.keyValues, 2);

        if (found.size() > 1 || (found.size() == 1 && !found.next().equals(key.rid)))
          throw new DuplicatedKeyException(i.index.getName(), Arrays.toString(key.keyValues), found.getRID());
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
