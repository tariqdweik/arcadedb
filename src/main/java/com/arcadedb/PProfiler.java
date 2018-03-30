package com.arcadedb;

import com.arcadedb.database.PDatabaseInternal;
import com.arcadedb.database.async.PDatabaseAsyncExecutor;
import com.arcadedb.engine.PFileManager;
import com.arcadedb.engine.PPageManager;
import com.arcadedb.utility.PFileUtils;

import javax.management.MBeanServer;
import javax.management.ObjectName;
import java.io.File;
import java.io.PrintStream;
import java.lang.management.GarbageCollectorMXBean;
import java.lang.management.ManagementFactory;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;

public class PProfiler {
  public static final PProfiler INSTANCE = new PProfiler();

  private Set<PDatabaseInternal> databases = new LinkedHashSet<>();

  protected PProfiler() {
  }

  public synchronized void registerDatabase(final PDatabaseInternal database) {
    databases.add(database);
  }

  public void unregisterDatabase(final PDatabaseInternal database) {
    databases.remove(database);
  }

  @Override
  protected void finalize() throws Throwable {
    if (!databases.isEmpty())
      System.err.println("PROTON: The following databases weren't closed properly: " + databases);
  }

  public synchronized void dumpMetrics(final PrintStream out) {

    final StringBuilder buffer = new StringBuilder("\n");

    final long freeSpaceInMB = new File(".").getFreeSpace();
    final long totalSpaceInMB = new File(".").getTotalSpace();

    long readCacheUsed = 0;
    long writeCacheUsed = 0;
    long cacheMax = 0;
    int pagesRead = 0;
    int pagesWritten = 0;
    long pagesReadSize = 0;
    long pagesWrittenSize = 0;
    long pageFlushQueueLength = 0;
    int asynchQueueLength = 0;
    long pageCacheHits = 0;
    long pageCacheMiss = 0;
    long totalOpenFiles = 0;
    long maxOpenFiles = 0;
    long walPagesWritten = 0;
    long walBytesWritten = 0;
    long walTotalFiles = 0;

    for (PDatabaseInternal db : databases) {
      final PPageManager.PPageManagerStats pStats = db.getPageManager().getStats();
      readCacheUsed += pStats.readCacheRAM;
      writeCacheUsed += pStats.writeCacheRAM;
      cacheMax += pStats.maxRAM;
      pagesRead += pStats.pagesRead;
      pagesReadSize += pStats.pagesReadSize;
      pagesWritten += pStats.pagesWritten;
      pagesWrittenSize += pStats.pagesWrittenSize;
      pageFlushQueueLength += pStats.pageFlushQueueLength;
      pageCacheHits += pStats.cacheHits;
      pageCacheMiss += pStats.cacheMiss;

      final PFileManager.PFileManagerStats fStats = db.getFileManager().getStats();
      totalOpenFiles += fStats.totalOpenFiles;
      maxOpenFiles += fStats.maxOpenFiles;

      final PDatabaseAsyncExecutor.PDBAsynchStats aStats = db.asynch().getStats();
      asynchQueueLength += aStats.queueSize;

      final Map<String, Object> walStats = db.getTransactionManager().getStats();
      walPagesWritten += (Long) walStats.get("pagesWritten");
      walBytesWritten += (Long) walStats.get("bytesWritten");
      walTotalFiles += (Long) walStats.get("logFiles");
    }

    buffer.append(String.format("PROTON %s Profiler", PConstants.getVersion()));

    final Runtime runtime = Runtime.getRuntime();

    final long gcTime = getGarbageCollectionTime();

    try {
      MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();
      ObjectName osMBeanName = ObjectName.getInstance(ManagementFactory.OPERATING_SYSTEM_MXBEAN_NAME);

      if (mbs.isInstanceOf(osMBeanName, "com.sun.management.OperatingSystemMXBean")) {
        final long osTotalMem = ((Number) mbs.getAttribute(osMBeanName, "TotalPhysicalMemorySize")).longValue();
        final long osUsedMem = osTotalMem - ((Number) mbs.getAttribute(osMBeanName, "FreePhysicalMemorySize")).longValue();

        buffer.append(String
            .format("\n JVM HEAP=%s/%s OS=%s/%s GC=%dms", PFileUtils.getSizeAsString(runtime.totalMemory() - runtime.freeMemory()),
                PFileUtils.getSizeAsString(runtime.maxMemory()), PFileUtils.getSizeAsString(osUsedMem),
                PFileUtils.getSizeAsString(osTotalMem), gcTime));

        buffer.append(String.format("\n DISKCACHE read=%s write=%s max=%s",
            PFileUtils.getSizeAsString(runtime.totalMemory() - runtime.freeMemory()),
            PFileUtils.getSizeAsString(runtime.maxMemory()), PFileUtils.getSizeAsString(readCacheUsed),
            PFileUtils.getSizeAsString(writeCacheUsed), PFileUtils.getSizeAsString(cacheMax)));
      }

    } catch (Exception e) {
      // JMX NOT AVAILABLE, AVOID OS DATA
      buffer.append(String.format("\n DISKCACHE read=%s write=%s max=%s", PFileUtils.getSizeAsString(readCacheUsed),
          PFileUtils.getSizeAsString(writeCacheUsed), PFileUtils.getSizeAsString(cacheMax)));
    }

    buffer.append(String.format("\n DB databases=%d asynchQueue=%d", databases.size(), asynchQueueLength));

    buffer.append(String.format("\n PAGE-MANAGER read=%d (%s) write=%d (%s) flushQueue=%d cacheHits=%d cacheMiss=%d", pagesRead,
        PFileUtils.getSizeAsString(pagesReadSize), pagesWritten, PFileUtils.getSizeAsString(pagesWrittenSize), pageFlushQueueLength,
        pageCacheHits, pageCacheMiss));

    buffer.append(String.format("\n WAL totalFiles=%d pagesWritten=%d bytesWritten=%s", walTotalFiles, walPagesWritten,
        PFileUtils.getSizeAsString(walBytesWritten)));

    buffer.append(String
        .format("\n FILE-MANAGER FS=%s/%s openFiles=%d maxFilesOpened=%d", PFileUtils.getSizeAsString(freeSpaceInMB),
            PFileUtils.getSizeAsString(totalSpaceInMB), totalOpenFiles, maxOpenFiles));

    out.println(buffer.toString());
  }

  private static long getGarbageCollectionTime() {
    long collectionTime = 0;
    for (GarbageCollectorMXBean garbageCollectorMXBean : ManagementFactory.getGarbageCollectorMXBeans()) {
      collectionTime += garbageCollectorMXBean.getCollectionTime();
    }
    return collectionTime;
  }
}
