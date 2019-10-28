/*
 * Copyright (c) - Arcade Analytics LTD (https://arcadeanalytics.com)
 */

package com.arcadedb.database;

import com.arcadedb.engine.Bucket;
import com.arcadedb.index.Index;
import com.arcadedb.schema.DocumentType;

import java.util.List;

public class DocumentIndexer {
  private final EmbeddedDatabase database;

  protected DocumentIndexer(final EmbeddedDatabase database) {
    this.database = database;
  }

  public List<Index> getInvolvedIndexes(final Document modifiedRecord) {
    if (modifiedRecord == null)
      throw new IllegalArgumentException("Modified record is null");

    final RID rid = modifiedRecord.getIdentity();
    if (rid == null)
      // RECORD IS NOT PERSISTENT
      return null;

    final int bucketId = rid.getBucketId();

    final DocumentType type = database.getSchema().getType(modifiedRecord.getType());

    return type.getSubIndexByBucketId(bucketId);
  }

  public void createDocument(final Document record, final DocumentType type, final Bucket bucket) {
    final RID rid = record.getIdentity();
    if (rid == null)
      throw new IllegalArgumentException("Cannot index a non persistent record");

    // INDEX THE RECORD
    final List<Index> metadata = type.getSubIndexByBucketId(bucket.getId());
    if (metadata != null) {
      for (Index entry : metadata) {
        final Index index = entry;
        final String[] keyNames = entry.getPropertyNames();

        final Object[] keyValues = new Object[keyNames.length];
        for (int i = 0; i < keyValues.length; ++i)
          keyValues[i] = record.get(keyNames[i]);

        index.put(keyValues, new RID[] { rid });
      }
    }
  }

  public void updateDocument(final Document originalRecord, final Document modifiedRecord, final List<Index> indexes) {
    if (indexes == null || indexes.isEmpty())
      return;

    if (originalRecord == null)
      throw new IllegalArgumentException("Original record is null");
    if (modifiedRecord == null)
      throw new IllegalArgumentException("Modified record is null");

    final RID rid = modifiedRecord.getIdentity();
    if (rid == null)
      // RECORD IS NOT PERSISTENT
      return;

    for (Index index : indexes) {
      final String[] keyNames = index.getPropertyNames();
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
      index.put(newKeyValues, new RID[] { rid });
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

    final List<Index> metadata = type.getSubIndexByBucketId(bucketId);
    if (metadata != null) {
      if (record instanceof RecordInternal)
        // FORCE RESET OF ANY PROPERTY TEMPORARY SET
        ((RecordInternal) record).unsetDirty();

      for (Index index : metadata) {
        final String[] keyNames = index.getPropertyNames();
        final Object[] keyValues = new Object[keyNames.length];
        for (int i = 0; i < keyNames.length; ++i) {
          keyValues[i] = record.get(keyNames[i]);
        }

        index.remove(keyValues, record.getIdentity());
      }
    }
  }
}
