package com.arcadedb.engine;

import com.arcadedb.PGlobalConfiguration;
import com.arcadedb.utility.PLogManager;

import java.io.IOException;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.TimeUnit;

/**
 * Flushes pages to disk asynchronously.
 */
public class PPageManagerFlushThread extends Thread {
  private final    PPageManager                        pageManager;
  public final     ArrayBlockingQueue<PModifiablePage> queue   = new ArrayBlockingQueue<>(
      PGlobalConfiguration.PAGE_FLUSH_QUEUE.getValueAsInteger());
  private volatile boolean                             running = true;

  public PPageManagerFlushThread(final PPageManager pageManager) {
    super("AsynchFlush");
    this.pageManager = pageManager;
  }

  public void asyncFlush(final PModifiablePage page) throws InterruptedException {
    PLogManager.instance().debug(this, "Enqueuing flushing page %s in bg...", page);
    queue.put(page);
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
    final PModifiablePage page = queue.poll(300l, TimeUnit.MILLISECONDS);

    if (page != null) {
      if (PLogManager.instance().isDebugEnabled())
        PLogManager.instance().debug(this, "Flushing page %s in bg...", page);

      pageManager.flushPage(page);
    }
  }

  public void close() {
    running = false;
  }
}
