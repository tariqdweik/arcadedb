/*
 * Copyright (c) 2018 - Arcade Analytics LTD (https://arcadeanalytics.com)
 */

package com.arcadedb.database;

import com.arcadedb.engine.Bucket;
import com.arcadedb.exception.DuplicatedKeyException;
import com.arcadedb.index.Index;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.Schema;

import java.util.*;

public class TransactionIndexContext {
  private final DatabaseInternal            database;
  private final Map<String, List<IndexKey>> indexKeysToLocks = new HashMap<>();

  public TransactionIndexContext(final DatabaseInternal database) {
    this.database = database;
  }

  public void checkUniqueIndexKeys() {
    for (Map.Entry<String, List<IndexKey>> entry : indexKeysToLocks.entrySet()) {
      final Index index = database.getSchema().getIndexByName(entry.getKey());
      final List<IndexKey> keys = entry.getValue();
      for (int i = 0; i < keys.size(); ++i)
        checkUniqueIndexKeys(index, keys.get(i));
    }
    indexKeysToLocks.clear();
  }

  public void addFilesToLock(Set<Integer> modifiedFiles) {
    final Schema schema = database.getSchema();

    final Set<Index> lockedIndexes = new HashSet<>();

    for (Map.Entry<String, List<IndexKey>> entry : indexKeysToLocks.entrySet()) {
      final Index index = schema.getIndexByName(entry.getKey());

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

  public Map<String, List<IndexKey>> toMap() {
    return indexKeysToLocks;
  }

  public static class IndexKey {
    public final Object[] keyValues;
    public final RID      rid;

    public IndexKey(final Object[] keyValues, final RID rid) {
      this.keyValues = keyValues;
      this.rid = rid;
    }

    @Override
    public String toString() {
      return "IndexKey(" + Arrays.toString(keyValues) + ")";
    }
  }

  public void addAll(final Map<String, List<IndexKey>> keysTx) {
    indexKeysToLocks.clear();
    indexKeysToLocks.putAll(keysTx);
  }

  public boolean isEmpty() {
    return indexKeysToLocks.isEmpty();
  }

  public void addIndexKeyLock(final String indexName, final Object[] keysValues, final RID rid) {
    List<IndexKey> keys = indexKeysToLocks.get(indexName);
    if (keys == null) {
      keys = new ArrayList<>();
      indexKeysToLocks.put(indexName, keys);
    }
    keys.add(new IndexKey(keysValues, rid));
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
  private void checkUniqueIndexKeys(final Index index, final IndexKey key) {
    final DocumentType type = database.getSchema().getType(index.getTypeName());

    // CHECK UNIQUENESS ACROSS ALL THE INDEXES FOR ALL THE BUCKETS
    final List<DocumentType.IndexMetadata> typeIndexes = type.getIndexMetadataByProperties(index.getPropertyNames());
    if (typeIndexes != null) {
      for (DocumentType.IndexMetadata i : typeIndexes) {
        final Set<RID> found = i.index.get(key.keyValues, 2);

        if (found.size() > 1 || (found.size() == 1 && !found.iterator().next().equals(key.rid)))
          throw new DuplicatedKeyException(i.index.getName(), Arrays.toString(key.keyValues), found.iterator().next());
      }
    }
  }
}
