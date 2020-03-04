/*
 * Copyright (c) - Arcade Data LTD (https://arcadedata.com)
 */

package com.arcadedb.utility;

import java.util.concurrent.Callable;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class RWDynamicLockContext {
  private final ReentrantReadWriteLock lock;

  public RWDynamicLockContext(final boolean multiThread) {
    lock = multiThread ? new ReentrantReadWriteLock() : null;
  }

  protected void readLock() {
    if (lock != null)
      lock.readLock().lock();
  }

  protected void readUnlock() {
    if (lock != null)
      lock.readLock().unlock();
  }

  protected void writeLock() {
    if (lock != null)
      lock.writeLock().lock();
  }

  protected void writeUnlock() {
    if (lock != null)
      lock.writeLock().unlock();
  }

  public Object executeInReadLock(Callable<Object> callable) {
    readLock();
    try {

      return callable.call();

    } catch (RuntimeException e) {
      throw e;

    } catch (Throwable e) {
      throw new RuntimeException("Error in execution in lock", e);

    } finally {
      readUnlock();
    }
  }

  public Object executeInWriteLock(Callable<Object> callable) {
    writeLock();
    try {

      return callable.call();

    } catch (RuntimeException e) {
      throw e;

    } catch (Throwable e) {
      throw new RuntimeException("Error in execution in lock", e);

    } finally {
      writeUnlock();
    }
  }
}
