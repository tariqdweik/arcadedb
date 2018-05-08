/*
 * Copyright (c) 2018 - Arcade Analytics LTD (https://arcadeanalytics.com)
 */

package com.arcadedb.engine;

import com.arcadedb.database.Database;
import com.arcadedb.database.RID;
import com.arcadedb.database.Record;
import com.arcadedb.exception.DatabaseOperationException;

import java.io.IOException;
import java.util.Iterator;

import static com.arcadedb.database.Binary.SHORT_SERIALIZED_SIZE;

public class BucketIterator implements Iterator<Record> {

  private final Database database;
  private final Bucket   bucket;
  int      nextPageNumber      = 0;
  BasePage currentPage         = null;
  short    recordCountInCurrentPage;
  int      totalPages;
  Record   next                = null;
  int      currentRecordInPage = 0;

  BucketIterator(Bucket bucket, Database db) throws IOException {
    this.bucket = bucket;
    this.database = db;
    this.totalPages = bucket.pageCount.get();

    fetchNext();
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
          recordCountInCurrentPage = currentPage.readShort(bucket.PAGE_RECORD_COUNT_IN_PAGE_OFFSET);
        }

        if (recordCountInCurrentPage > 0 && currentRecordInPage < recordCountInCurrentPage) {
          final int recordPositionInPage = currentPage
              .readUnsignedShort(bucket.PAGE_RECORD_TABLE_OFFSET + currentRecordInPage * SHORT_SERIALIZED_SIZE);

          final int recordSize = currentPage.readUnsignedShort(recordPositionInPage);

          if (recordSize > 0) {
            // NOT DELETED
            final RID rid = new RID(database, bucket.id, nextPageNumber * bucket.MAX_RECORDS_IN_PAGE + currentRecordInPage);

            currentRecordInPage++;

            if (!bucket.existsRecord(rid))
              continue;

            final Record record = rid.getRecord();
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
