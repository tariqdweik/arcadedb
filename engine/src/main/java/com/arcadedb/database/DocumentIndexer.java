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

  private final EmbeddedDatabase database;

  protected DocumentIndexer(final EmbeddedDatabase database) {
    this.database = database;
  }

  public void createDocument(final ModifiableDocument record, final DocumentType type, final Bucket bucket) {
    // INDEX THE RECORD
    final List<DocumentType.IndexMetadata> metadata = type.getIndexMetadataByBucketId(bucket.getId());
    if (metadata != null) {
      for (DocumentType.IndexMetadata entry : metadata) {
        final Index index = entry.index;
        final String[] keyNames = entry.propertyNames;
        final Object[] keyValues = new Object[keyNames.length];
        for (int i = 0; i < keyNames.length; ++i) {
          keyValues[i] = record.get(keyNames[i]);
        }

        if (index.isUnique()) {
          // CHECK UNIQUENESS ACROSS ALL THE INDEXES FOR ALL THE BUCKETS
          final List<DocumentType.IndexMetadata> typeIndexes = type.getIndexMetadataByProperties(entry.propertyNames);
          if (typeIndexes != null) {
            for (DocumentType.IndexMetadata i : typeIndexes) {
              if (!i.index.get(keyValues).isEmpty())
                throw new DuplicatedKeyException(i.index.getName(), Arrays.toString(keyValues));
            }
          }
        }

        index.put(keyValues, record.getIdentity());
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

          if (newKeyValues == null || !newKeyValues.equals(oldKeyValues[i])) {
            keyValuesAreModified = true;
            break;
          }
        }

        if (!keyValuesAreModified)
          // SAME VALUES, SKIP INDEX UPDATE
          continue;

        // REMOVE THE OLD ENTRY KEYS/VALUE AND INSERT THE NEW ONE
        index.remove(oldKeyValues, rid);
        index.put(newKeyValues, rid);
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
}
