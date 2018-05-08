/*
 * Copyright (c) 2018 - Arcade Analytics LTD (https://arcadeanalytics.com)
 */

package com.arcadedb.utility;

import java.util.concurrent.Callable;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class LockContext {
  private final Lock lock;

  public LockContext(final boolean multiThread) {
    lock = multiThread ? new ReentrantLock() : null;
  }

  protected void lock() {
    if (lock != null)
      lock.lock();
  }

  protected void unlock() {
    if (lock != null)
      lock.unlock();
  }

  public Object executeInLock(Callable<Object> callable) {
    if (lock != null)
      lock.lock();
    try {

      return callable.call();

    } catch (RuntimeException e) {
      throw e;

    } catch (Throwable e) {
      throw new RuntimeException("Error in execution in lock", e);

    } finally {
      if (lock != null)
        lock.unlock();
    }
  }
}
