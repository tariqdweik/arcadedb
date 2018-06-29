/*
 * Copyright (c) 2018 - Arcade Analytics LTD (https://arcadeanalytics.com)
 */

package com.arcadedb.utility;

import java.util.concurrent.Callable;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class LockContext {
  private final Lock lock = new ReentrantLock();

  protected void lock() {
    lock.lock();
  }

  protected void unlock() {
    lock.unlock();
  }

  protected RuntimeException manageExceptionInLock(final Throwable e) {
    if (e instanceof RuntimeException)
      throw (RuntimeException) e;

    return new RuntimeException("Error in execution in lock", e);
  }

  public Object executeInLock(final Callable<Object> callable) {
    lock.lock();
    try {

      return callable.call();

    } catch (Throwable e) {
      throw manageExceptionInLock(e);

    } finally {
      lock.unlock();
    }
  }
}
