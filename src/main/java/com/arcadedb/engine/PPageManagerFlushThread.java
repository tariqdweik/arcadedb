package com.arcadedb.engine;

import com.arcadedb.utility.PLogManager;

import java.io.IOException;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.TimeUnit;

/**
 * Flushes pages to disk asynchronously.
 */
public class PPageManagerFlushThread extends Thread {
  private final PPageManager pageManager;
  public final     ArrayBlockingQueue<PPageId> queue   = new ArrayBlockingQueue<>(8192);
  private volatile boolean                     running = true;

  public PPageManagerFlushThread(final PPageManager pageManager) {
    super("AsynchFlush");
    this.pageManager = pageManager;
  }

  public void asyncFlush(final PPageId pageId) throws InterruptedException {
    PLogManager.instance().debug(this, "Enqueuing flushing page %s in bg...", pageId);
    queue.put(pageId);
  }

  @Override
  public void run() {
    while (running || !queue.isEmpty()) {
      try {
        flushStream();

      } catch (InterruptedException e) {
        running = false;
        return;
      } catch (Exception e) {
        PLogManager.instance().error(this, "Error on processing page flush requests", e);
        running = false;
        return;
      }
    }
  }

  private void flushStream() throws InterruptedException, IOException {
    final PPageId pageId = queue.poll(300l, TimeUnit.MILLISECONDS);

    if (pageId != null) {
      if (PLogManager.instance().isDebugEnabled())
        PLogManager.instance().debug(this, "Flushing page %s in bg...", pageId);

      pageManager.flushPage(pageId);
    }
  }

  public void close() {
    running = false;
  }
}
