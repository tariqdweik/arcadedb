/*
 * Copyright (c) 2018 - Arcade Analytics LTD (https://arcadeanalytics.com)
 */

package com.arcadedb.database;

import com.arcadedb.engine.Bucket;
import com.arcadedb.exception.DuplicatedKeyException;
import com.arcadedb.index.Index;
import com.arcadedb.schema.DocumentType;

import java.util.*;

public class TransactionIndexContext {
  private final DatabaseInternal            database;
  private final Map<String, List<IndexKey>> indexKeysToLocks = new HashMap<>();

  public TransactionIndexContext(final DatabaseInternal database) {
    this.database = database;
  }

  public void applyChangesToIndexes() {
    for (Map.Entry<String, List<IndexKey>> entry : indexKeysToLocks.entrySet()) {
      final Index index = database.getSchema().getIndexByName(entry.getKey());
      final List<IndexKey> keys = entry.getValue();
      for (int i = 0; i < keys.size(); ++i)
        applyIndexChangesAtCommit(index, keys.get(i));
    }
    indexKeysToLocks.clear();
  }

  public void addFilesToLock(Set<Integer> modifiedFiles) {
    for (Map.Entry<String, List<IndexKey>> entry : indexKeysToLocks.entrySet()) {
      final Index index = database.getSchema().getIndexByName(entry.getKey());

      modifiedFiles.add(index.getFileId());

      if (index.isUnique()) {
        // LOCK ALL THE FILES IMPACTED BY THE INDEX KEYS TO CHECK FOR UNIQUE CONSTRAINT
        final DocumentType type = database.getSchema().getType(index.getTypeName());
        final List<Bucket> buckets = type.getBuckets(false);
        for (Bucket b : buckets)
          modifiedFiles.add(b.getId());
      } else
        modifiedFiles.add(index.getAssociatedBucketId());
    }
  }

  public Map<String, List<IndexKey>> toMap() {
    return indexKeysToLocks;
  }

  public static class IndexKey {
    public final boolean  add;
    public final Object[] keyValues;
    public final RID      rid;

    public IndexKey(final boolean add, final Object[] keyValues, final RID rid) {
      this.add = add;
      this.keyValues = keyValues;
      this.rid = rid;
    }

    @Override
    public String toString() {
      return "IndexKey(" + (add ? "add" : "del") + "=" + Arrays.toString(keyValues) + ")";
    }
  }

  public void addAll(final Map<String, List<IndexKey>> keysTx) {
    indexKeysToLocks.clear();
    indexKeysToLocks.putAll(keysTx);
  }

  public boolean isEmpty() {
    return indexKeysToLocks.isEmpty();
  }

  public void addIndexKeyLock(final String indexName, final boolean add, final Object[] keysValues, final RID rid) {
    List<IndexKey> keys = indexKeysToLocks.get(indexName);
    if (keys == null) {
      keys = new ArrayList<>();
      indexKeysToLocks.put(indexName, keys);
    }
    keys.add(new IndexKey(add, keysValues, rid));
  }

  public void reset() {
    indexKeysToLocks.clear();
  }

  public List<IndexKey> getIndexKeys(final String indexName) {
    return indexKeysToLocks.get(indexName);
  }

  /**
   * Called at commit time in the middle of the lock to avoid concurrent insertion of the same key.
   */
  private void applyIndexChangesAtCommit(final Index index, final IndexKey key) {
    if (key.add) {
      if (index.isUnique()) {
        final DocumentType type = database.getSchema().getType(index.getTypeName());

        // CHECK UNIQUENESS ACROSS ALL THE INDEXES FOR ALL THE BUCKETS
        final List<DocumentType.IndexMetadata> typeIndexes = type.getIndexMetadataByProperties(index.getPropertyNames());
        if (typeIndexes != null) {
          for (DocumentType.IndexMetadata i : typeIndexes) {
            final Set<RID> found = i.index.get(key.keyValues, 1);
            if (!found.isEmpty())
              throw new DuplicatedKeyException(i.index.getName(), Arrays.toString(key.keyValues), found.iterator().next());
          }
        }
      }

      // AVOID CHECKING FOR UNIQUENESS BECAUSE IT HAS ALREADY BEEN CHECKED
      index.put(key.keyValues, key.rid);
    } else
      index.remove(key.keyValues, key.rid);
  }
}
