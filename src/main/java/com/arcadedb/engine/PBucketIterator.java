package com.arcadedb.engine;

import com.arcadedb.database.PDatabase;
import com.arcadedb.database.PRID;
import com.arcadedb.database.PRecord;
import com.arcadedb.exception.PDatabaseOperationException;

import java.io.IOException;
import java.util.Iterator;

import static com.arcadedb.database.PBinary.SHORT_SERIALIZED_SIZE;

public class PBucketIterator implements Iterator<PRecord> {

  private final PDatabase database;
  private final PBucket   bucket;
  int       nextPageNumber = 1;
  PBasePage currentPage    = null;
  short recordCountInCurrentPage;
  int   totalPages;
  PRecord next                = null;
  int     currentRecordInPage = 0;

  PBucketIterator(PBucket bucket, PDatabase db) throws IOException {
    this.bucket = bucket;
    this.database = db;
    this.totalPages = bucket.pageCount;

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
          currentPage = database.getTransaction().getPage(new PPageId(bucket.file.getFileId(), nextPageNumber), bucket.pageSize);
          recordCountInCurrentPage = currentPage.readShort(bucket.PAGE_RECORD_COUNT_IN_PAGE_OFFSET);
        }

        if (recordCountInCurrentPage > 0 && currentRecordInPage < recordCountInCurrentPage) {
          final int recordPositionInPage = currentPage
              .readUnsignedShort(bucket.PAGE_RECORD_TABLE_OFFSET + currentRecordInPage * SHORT_SERIALIZED_SIZE);

          final int recordSize = currentPage.readUnsignedShort(recordPositionInPage);

          if (recordSize > 0) {
            // NOT DELETED
            final PRID rid = new PRID(database, bucket.id, (nextPageNumber - 1) * bucket.MAX_RECORDS_IN_PAGE + currentRecordInPage);

            currentRecordInPage++;

            final PRecord record = rid.getRecord();
            next = record;
            return null;
          }

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
  public PRecord next() {
    if (next == null) {
      throw new IllegalStateException();
    }
    try {
      return next;
    } finally {
      try {
        fetchNext();
      } catch (Exception e) {
        throw new PDatabaseOperationException("Cannot scan bucket '" + bucket.name + "'", e);
      }
    }
  }
}
