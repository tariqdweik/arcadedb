/*
 * Copyright (c) - Arcade Analytics LTD (https://arcadeanalytics.com)
 */

package com.arcadedb.database.async;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

public abstract class DatabaseAsyncAbstractCallbackTask implements DatabaseAsyncTask {
  private final CountDownLatch semaphore = new CountDownLatch(1);

  public void waitForCompetition(final long timeoutInMs) throws InterruptedException {
    semaphore.await(timeoutInMs, TimeUnit.MILLISECONDS);
  }

  public void completed() {
    semaphore.countDown();
  }
}
