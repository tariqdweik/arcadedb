/*
 * Copyright (c) 2018 - Arcade Analytics LTD (https://arcadeanalytics.com)
 */

package com.arcadedb.database;

import com.arcadedb.engine.Bucket;
import com.arcadedb.exception.DuplicatedKeyException;
import com.arcadedb.index.Index;
import com.arcadedb.schema.DocumentType;

import java.util.Arrays;
import java.util.List;

public class DocumentIndexer {

  public static class IndexKey {
    public final Index    index;
    public final String   typeName;
    public final String[] keyNames;
    public final Object[] keys;
    public final RID      rid;

    public IndexKey(final Index index, final String typeName, final String[] keyNames, final Object[] keys, final RID rid) {
      this.index = index;
      this.typeName = typeName;
      this.keyNames = keyNames;
      this.keys = keys;
      this.rid = rid;
    }

    @Override
    public String toString() {
      return "IndexKey(" + typeName + Arrays.toString(keyNames) + "=" + Arrays.toString(keys) + ")";
    }
  }

  private final EmbeddedDatabase database;

  protected DocumentIndexer(final EmbeddedDatabase database) {
    this.database = database;
  }

  public void createDocument(final ModifiableDocument record, final DocumentType type, final Bucket bucket) {
    final RID rid = record.getIdentity();
    if (rid == null)
      throw new IllegalArgumentException("Cannot index a non persistent record");

    // INDEX THE RECORD
    final List<DocumentType.IndexMetadata> metadata = type.getIndexMetadataByBucketId(bucket.getId());
    if (metadata != null) {
      for (DocumentType.IndexMetadata entry : metadata) {
        final Index index = entry.index;
        final String[] keyNames = entry.propertyNames;
        final Object[] keyValues = new Object[keyNames.length];
        for (int i = 0; i < keyNames.length; ++i)
          keyValues[i] = record.get(keyNames[i]);

        if (index.isUnique())
          postponeUniqueInsertion(index, type.getName(), keyNames, keyValues, rid);
        else
          index.put(keyValues, rid, false);
      }
    }
  }

  public void updateDocument(final Document originalRecord, final Document modifiedRecord) {
    if (originalRecord == null)
      throw new IllegalArgumentException("Original record is null");
    if (modifiedRecord == null)
      throw new IllegalArgumentException("Modified record is null");

    final RID rid = modifiedRecord.getIdentity();
    if (rid == null)
      // RECORD IS NOT PERSISTENT
      return;

    final int bucketId = rid.getBucketId();

    final DocumentType type = database.getSchema().getType(modifiedRecord.getType());

    final List<DocumentType.IndexMetadata> metadata = type.getIndexMetadataByBucketId(bucketId);
    if (metadata != null) {
      for (DocumentType.IndexMetadata entry : metadata) {
        final Index index = entry.index;
        final String[] keyNames = entry.propertyNames;
        final Object[] oldKeyValues = new Object[keyNames.length];
        final Object[] newKeyValues = new Object[keyNames.length];

        boolean keyValuesAreModified = false;
        for (int i = 0; i < keyNames.length; ++i) {
          oldKeyValues[i] = originalRecord.get(keyNames[i]);
          newKeyValues[i] = modifiedRecord.get(keyNames[i]);

          if (newKeyValues[i] == null || !newKeyValues[i].equals(oldKeyValues[i])) {
            keyValuesAreModified = true;
            break;
          }
        }

        if (!keyValuesAreModified)
          // SAME VALUES, SKIP INDEX UPDATE
          continue;

        // REMOVE THE OLD ENTRY KEYS/VALUE AND INSERT THE NEW ONE
        index.remove(oldKeyValues, rid);

        if (index.isUnique())
          postponeUniqueInsertion(index, type.getName(), keyNames, newKeyValues, rid);
        else
          index.put(newKeyValues, rid, false);
      }
    }
  }

  public void deleteDocument(final Document record) {
    if (record.getIdentity() == null)
      // RECORD IS NOT PERSISTENT
      return;

    final int bucketId = record.getIdentity().getBucketId();

    final DocumentType type = database.getSchema().getTypeByBucketId(bucketId);
    if (type == null)
      throw new IllegalStateException("Type not found for bucket " + bucketId);

    final List<DocumentType.IndexMetadata> metadata = type.getIndexMetadataByBucketId(bucketId);
    if (metadata != null) {
      if (record instanceof RecordInternal)
        // FORCE RESET OF ANY PROPERTY TEMPORARY SET
        ((RecordInternal) record).unsetDirty();

      for (DocumentType.IndexMetadata entry : metadata) {
        final Index index = entry.index;
        final String[] keyNames = entry.propertyNames;
        final Object[] keyValues = new Object[keyNames.length];
        for (int i = 0; i < keyNames.length; ++i) {
          keyValues[i] = record.get(keyNames[i]);
        }

        index.remove(keyValues, record.getIdentity());
      }
    }
  }

  private void postponeUniqueInsertion(final Index index, final String typeName, final String[] keyNames, final Object[] keyValues, final RID rid) {
    // ADD THE KEY TO CHECK AT COMMIT TIME DURING THE LOCK
    database.getTransaction().addIndexKeyLock(new IndexKey(index, typeName, keyNames, keyValues, rid));
  }

  /**
   * Called at commit time in the middle of the lock to avoid concurrent insertion of the same key.
   */
  public void indexUniqueInsertionInTx(final IndexKey key) {
    final DocumentType type = database.getSchema().getType(key.typeName);

    // CHECK UNIQUENESS ACROSS ALL THE INDEXES FOR ALL THE BUCKETS
    final List<DocumentType.IndexMetadata> typeIndexes = type.getIndexMetadataByProperties(key.keyNames);
    if (typeIndexes != null) {
      for (DocumentType.IndexMetadata i : typeIndexes) {
        if (!i.index.get(key.keys, 1).isEmpty())
          throw new DuplicatedKeyException(i.index.getName(), Arrays.toString(key.keys));
      }
    }

    // AVOID CHECKING FOR UNIQUENESS BECAUSE IT HAS ALREADY BEEN CHECKED
    key.index.put(key.keys, key.rid, false);
  }
}
