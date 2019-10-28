/*
 * Copyright (c) - Arcade Analytics LTD (https://arcadeanalytics.com)
 */

package com.arcadedb.utility;

import java.util.concurrent.Callable;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class RWLockContext {
  private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();

  protected void readLock() {
    lock.readLock().lock();
  }

  protected void readUnlock() {
    lock.readLock().unlock();
  }

  protected void writeLock() {
    lock.writeLock().lock();
  }

  protected void writeUnlock() {
    lock.writeLock().unlock();
  }

  /**
   * Executes a callback in an shared lock.
   */
  public <RET extends Object> RET executeInReadLock(final Callable<RET> callable) {
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

  /**
   * Executes a callback in an exclusive lock.
   */
  public <RET extends Object> RET executeInWriteLock(final Callable<RET> callable) {
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
