package com.arcadedb.database.async;

import com.arcadedb.database.PDocumentCallback;
import com.arcadedb.engine.PBucket;

import java.util.concurrent.CountDownLatch;

public class PDatabaseAsyncScanType extends PDatabaseAsyncCommand {
  public final CountDownLatch    semaphore;
  public final PDocumentCallback userCallback;
  public final PBucket           bucket;

  public PDatabaseAsyncScanType(final CountDownLatch semaphore, final PDocumentCallback userCallback, final PBucket bucket) {
    super(null, null);

    this.semaphore = semaphore;
    this.userCallback = userCallback;
    this.bucket = bucket;
  }
}
