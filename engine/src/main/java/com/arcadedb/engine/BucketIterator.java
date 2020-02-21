/*
 * Copyright (c) - Arcade Analytics LTD (https://arcadeanalytics.com)
 */

package com.arcadedb.engine;

import com.arcadedb.database.*;
import com.arcadedb.exception.DatabaseOperationException;

import java.io.IOException;
import java.util.Iterator;

import static com.arcadedb.database.Binary.INT_SERIALIZED_SIZE;

public class BucketIterator implements Iterator<Record> {

  private final DatabaseInternal database;
  private final Bucket           bucket;
  int      nextPageNumber      = 0;
  BasePage currentPage         = null;
  short    recordCountInCurrentPage;
  int      totalPages;
  Record   next                = null;
  int      currentRecordInPage = 0;

  BucketIterator(Bucket bucket, Database db) {
    this.bucket = bucket;
    this.database = (DatabaseInternal) db;
    this.totalPages = bucket.pageCount.get();

    fetchNext();
  }

  public void setPosition(final RID position) throws IOException {
    next = position.getRecord();
    nextPageNumber = (int) (position.getPosition() / Bucket.MAX_RECORDS_IN_PAGE);
    currentRecordInPage = (int) (position.getPosition() % Bucket.MAX_RECORDS_IN_PAGE) + 1;
    currentPage = database.getTransaction().getPage(new PageId(position.getBucketId(), nextPageNumber), bucket.pageSize);
    recordCountInCurrentPage = currentPage.readShort(Bucket.PAGE_RECORD_COUNT_IN_PAGE_OFFSET);
  }

  private void fetchNext() {
    database.executeInReadLock(() -> {
      next = null;
      while (true) {
        if (currentPage == null) {
          if (nextPageNumber > totalPages) {
            return null;
          }
          currentPage = database.getTransaction().getPage(new PageId(bucket.file.getFileId(), nextPageNumber), bucket.pageSize);
          recordCountInCurrentPage = currentPage.readShort(Bucket.PAGE_RECORD_COUNT_IN_PAGE_OFFSET);
        }

        if (recordCountInCurrentPage > 0 && currentRecordInPage < recordCountInCurrentPage) {
          int recordPositionInPage = (int) currentPage.readUnsignedInt(Bucket.PAGE_RECORD_TABLE_OFFSET + currentRecordInPage * INT_SERIALIZED_SIZE);
          final long recordSize[] = currentPage.readNumberAndSize(recordPositionInPage);
          if (recordSize[0] > 0) {
            // NOT DELETED
            final RID rid = new RID(database, bucket.id, nextPageNumber * Bucket.MAX_RECORDS_IN_PAGE + currentRecordInPage);

            currentRecordInPage++;

            if (!bucket.existsRecord(rid))
              continue;

            final Record record = rid.getRecord(false);
            next = record;
            return null;

          } else if (recordSize[0] == -1) {
            // PLACEHOLDER
            final RID rid = new RID(database, bucket.id, nextPageNumber * Bucket.MAX_RECORDS_IN_PAGE + currentRecordInPage);

            currentRecordInPage++;

            final Binary view = bucket
                .getRecordInternal(new RID(database, bucket.id, currentPage.readLong((int) (recordPositionInPage + recordSize[1]))), true);

            if (view == null)
              continue;

            final Record record = database.getRecordFactory()
                .newImmutableRecord(database, database.getSchema().getTypeNameByBucketId(rid.getBucketId()), rid, view);

            next = record;
            return null;
          }

          currentRecordInPage++;

        } else if (currentRecordInPage == recordCountInCurrentPage) {
          currentRecordInPage = 0;
          currentPage = null;
          nextPageNumber++;
        } else {
          currentRecordInPage++;
        }
      }
    });
  }

  @Override
  public boolean hasNext() {
    return next != null;
  }

  @Override
  public Record next() {
    if (next == null) {
      throw new IllegalStateException();
    }
    try {
      return next;
    } finally {
      try {
        fetchNext();
      } catch (Exception e) {
        throw new DatabaseOperationException("Cannot scan bucket '" + bucket.name + "'", e);
      }
    }
  }
}
