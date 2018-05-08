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
