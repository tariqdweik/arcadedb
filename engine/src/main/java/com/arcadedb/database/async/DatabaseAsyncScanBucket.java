/*
 * Copyright (c) 2018 - Arcade Analytics LTD (https://arcadeanalytics.com)
 */

package com.arcadedb.database.async;

import com.arcadedb.database.DocumentCallback;
import com.arcadedb.engine.Bucket;

import java.util.concurrent.CountDownLatch;

public class DatabaseAsyncScanBucket implements DatabaseAsyncCommand {
  public final CountDownLatch   semaphore;
  public final DocumentCallback userCallback;
  public final Bucket           bucket;

  public DatabaseAsyncScanBucket(final CountDownLatch semaphore, final DocumentCallback userCallback, final Bucket bucket) {
    this.semaphore = semaphore;
    this.userCallback = userCallback;
    this.bucket = bucket;
  }

  @Override
  public String toString() {
    return "ScanBucket(" + bucket + ")";
  }
}
