package com.arcadedb.database.async;

import com.arcadedb.database.DocumentCallback;
import com.arcadedb.engine.Bucket;

import java.util.concurrent.CountDownLatch;

public class DatabaseAsyncScanType extends DatabaseAsyncCommand {
  public final CountDownLatch   semaphore;
  public final DocumentCallback userCallback;
  public final Bucket           bucket;

  public DatabaseAsyncScanType(final CountDownLatch semaphore, final DocumentCallback userCallback, final Bucket bucket) {
    this.semaphore = semaphore;
    this.userCallback = userCallback;
    this.bucket = bucket;
  }
}
