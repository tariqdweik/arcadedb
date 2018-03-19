package com.arcadedb.engine;

import com.arcadedb.utility.PLogManager;

import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

/**
 * Flushes pages to disk asynchronously.
 */
public class PPageManagerFlushThread extends Thread {
  private final PPageManager pageManager;
  private final    LinkedBlockingQueue<PModifiablePage> queue   = new LinkedBlockingQueue<PModifiablePage>();
  private volatile boolean                              running = true;

  public PPageManagerFlushThread(final PPageManager pageManager) {
    this.pageManager = pageManager;
  }

  public void asyncFlush(final PModifiablePage page) throws InterruptedException {
    PLogManager.instance().debug(this, "Enqueuing flushing page %s in bg (size=%d)...", page.pageId, page.getPhysicalSize());
    queue.put(page);
  }

  @Override
  public void run() {
    while (running || !queue.isEmpty()) {
      try {
        final PModifiablePage page = queue.poll(300l, TimeUnit.MILLISECONDS);

        if (page != null) {
          if (PLogManager.instance().isDebugEnabled())
            PLogManager.instance().debug(this, "Flushing page %s in bg (size=%d)...", page.pageId, page.getPhysicalSize());
          pageManager.flushPage(page);
        }

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

  public void close() {
    running = false;
  }
}
