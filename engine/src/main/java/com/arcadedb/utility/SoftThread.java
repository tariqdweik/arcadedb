/*
 * Copyright (c) 2018 - Arcade Analytics LTD (https://arcadeanalytics.com)
 */

package com.arcadedb.utility;

import com.arcadedb.log.LogManager;

import java.util.logging.Level;

public abstract class SoftThread extends Thread {
  private volatile boolean shutdownFlag;

  private boolean dumpExceptions = true;

  public SoftThread(final ThreadGroup iThreadGroup) {
    super(iThreadGroup, SoftThread.class.getSimpleName());
    setDaemon(true);
  }

  public SoftThread(final String name) {
    super(name);
    setDaemon(true);
  }

  public SoftThread(final ThreadGroup group, final String name) {
    super(group, name);
    setDaemon(true);
  }

  protected abstract void execute() throws Exception;

  public void startup() {
  }

  public void shutdown() {
  }

  public void sendShutdown() {
    shutdownFlag = true;
    interrupt();
  }

  public void softShutdown() {
    shutdownFlag = true;
  }

  public boolean isShutdownFlag() {
    return shutdownFlag;
  }

  @Override
  public void run() {
    startup();

    while (!shutdownFlag && !isInterrupted()) {
      try {
        beforeExecution();
        execute();
        afterExecution();
      } catch (Exception e) {
        if (dumpExceptions)
          LogManager.instance().log(this, Level.SEVERE, "Error during thread execution", e);
      } catch (Error e) {
        if (dumpExceptions)
          LogManager.instance().log(this, Level.SEVERE, "Error during thread execution", e);
        throw e;
      }
    }

    shutdown();
  }

  /**
   * Pauses current thread until iTime timeout or a wake up by another thread.
   *
   * @return true if timeout has reached, otherwise false. False is the case of wake-up by another thread.
   */
  public static boolean pauseCurrentThread(long iTime) {
    try {
      if (iTime <= 0)
        iTime = Long.MAX_VALUE;

      Thread.sleep(iTime);
      return true;
    } catch (InterruptedException ignore) {
      Thread.currentThread().interrupt();
      return false;
    }
  }

  public boolean isDumpExceptions() {
    return dumpExceptions;
  }

  public void setDumpExceptions(final boolean dumpExceptions) {
    this.dumpExceptions = dumpExceptions;
  }

  protected void beforeExecution() {
    return;
  }

  protected void afterExecution() {
    return;
  }
}
