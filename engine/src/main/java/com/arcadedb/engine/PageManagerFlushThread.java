/*
 * Copyright (c) 2018 - Arcade Analytics LTD (https://arcadeanalytics.com)
 */

package com.arcadedb.engine;

import com.arcadedb.ContextConfiguration;
import com.arcadedb.GlobalConfiguration;
import com.arcadedb.utility.LogManager;

import java.io.IOException;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.TimeUnit;

/**
 * Flushes pages to disk asynchronously.
 */
public class PageManagerFlushThread extends Thread {
  private final    PageManager                     pageManager;
  public final     ArrayBlockingQueue<MutablePage> queue;
  private final    String                          logContext;
  private volatile boolean                         running = true;

  public PageManagerFlushThread(final PageManager pageManager, final ContextConfiguration configuration) {
    super("AsynchFlush");
    this.pageManager = pageManager;
    this.logContext = LogManager.instance().getContext();
    this.queue = new ArrayBlockingQueue<>(configuration.getValueAsInteger(GlobalConfiguration.PAGE_FLUSH_QUEUE));
  }

  public void asyncFlush(final MutablePage page) throws InterruptedException {
    LogManager.instance().debug(this, "Enqueuing flushing page %s in bg...", page);
    queue.put(page);
  }

  @Override
  public void run() {
    if (logContext != null)
      LogManager.instance().setContext(logContext);

    while (running || !queue.isEmpty()) {
      try {
        flushStream();

      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        running = false;
        return;
      } catch (Exception e) {
        LogManager.instance().error(this, "Error on processing page flush requests", e);
      }
    }
  }

  private void flushStream() throws InterruptedException, IOException {
    final MutablePage page = queue.poll(300l, TimeUnit.MILLISECONDS);

    if (page != null) {
      if (LogManager.instance().isDebugEnabled())
        LogManager.instance().debug(this, "Flushing page %s in bg...", page);

      pageManager.flushPage(page);
    }
  }

  public void close() {
    running = false;
  }
}
