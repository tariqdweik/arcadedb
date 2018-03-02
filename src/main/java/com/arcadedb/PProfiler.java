package com.arcadedb;

import com.arcadedb.database.PDatabase;
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
import java.util.Set;

public class PProfiler {
  public static final PProfiler INSTANCE = new PProfiler();

  private Set<PDatabase> databases = new LinkedHashSet<>();

  protected PProfiler() {
  }

  public synchronized Set<PDatabase> getDatabases() {
    return databases;
  }

  public synchronized void registerDatabase(final PDatabase database) {
    databases.add(database);
  }

  public void unregisterDatabase(final PDatabase database) {
    databases.remove(database);
  }

  @Override
  protected void finalize() throws Throwable {
    if (!databases.isEmpty())
      System.err.println("PROTON: The following databases weren't closed properly: " + databases);
  }

  public synchronized void dumpMetrics(final PrintStream out) {

    final StringBuilder buffer = new StringBuilder();

    final long freeSpaceInMB = new File(".").getFreeSpace();
    final long totalSpaceInMB = new File(".").getTotalSpace();

    long diskCacheUsed = 0;
    long diskCacheTotal = 0;
    long readRAM = 0;
    long writeRAM = 0;
    int pagesToDispose = 0;
    int pagesRead = 0;
    int pagesWritten = 0;
    long pagesReadSize = 0;
    long pagesWrittenSize = 0;

    long totalOpenFiles = 0;
    long maxOpenFiles = 0;

    for (PDatabase db : databases) {
      final PPageManager.PPageManagerStats pStats = db.getPageManager().getStats();
      diskCacheTotal += pStats.maxRAM;
      readRAM += pStats.totalImmutablePagesRAM;
      writeRAM += pStats.totalModifiedPagesRAM;
      pagesToDispose += pStats.pagesToDispose;
      pagesRead += pStats.pagesRead;
      pagesReadSize += pStats.pagesReadSize;
      pagesWritten += pStats.pagesWritten;
      pagesWrittenSize += pStats.pagesWrittenSize;

      final PFileManager.PFileManagerStats fStats = db.getFileManager().getStats();
      totalOpenFiles += fStats.totalOpenFiles;
      maxOpenFiles += fStats.maxOpenFiles;
    }
    diskCacheUsed = readRAM + writeRAM;

    final Runtime runtime = Runtime.getRuntime();

    final long gcTime = getGarbageCollectionTime();
    try {
      MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();
      ObjectName osMBeanName = ObjectName.getInstance(ManagementFactory.OPERATING_SYSTEM_MXBEAN_NAME);

      if (mbs.isInstanceOf(osMBeanName, "com.sun.management.OperatingSystemMXBean")) {
        final long osTotalMem = ((Number) mbs.getAttribute(osMBeanName, "TotalPhysicalMemorySize")).longValue();
        final long osUsedMem = osTotalMem - ((Number) mbs.getAttribute(osMBeanName, "FreePhysicalMemorySize")).longValue();

        buffer.append(String
            .format("PROTON %s Memory profiler: HEAP=%s/%s - DISKCACHE (%s dbs)=%s/%s - OS=%s/%s - FS=%s/%s - GC=%dms",
                PConstants.getVersion(), PFileUtils.getSizeAsString(runtime.totalMemory() - runtime.freeMemory()),
                PFileUtils.getSizeAsString(runtime.maxMemory()), databases.size(), PFileUtils.getSizeAsString(diskCacheUsed),
                PFileUtils.getSizeAsString(diskCacheTotal), PFileUtils.getSizeAsString(osUsedMem),
                PFileUtils.getSizeAsString(osTotalMem), PFileUtils.getSizeAsString(freeSpaceInMB),
                PFileUtils.getSizeAsString(totalSpaceInMB), gcTime));
      }

    } catch (Exception e) {
      // JMX NOT AVAILABLE, AVOID OS DATA
      buffer.append(String
          .format("PROTON %s Memory profiler: HEAP=%s/%s - DISKCACHE (%s dbs)=%s/%s - FS=%s/%s - GC=%dms", PConstants.getVersion(),
              PFileUtils.getSizeAsString(runtime.totalMemory() - runtime.freeMemory()),
              PFileUtils.getSizeAsString(runtime.maxMemory()), databases.size(), PFileUtils.getSizeAsString(diskCacheUsed),
              PFileUtils.getSizeAsString(diskCacheTotal), PFileUtils.getSizeAsString(freeSpaceInMB),
              PFileUtils.getSizeAsString(totalSpaceInMB), gcTime));
    }

    buffer.append(String.format("\n PageManager read=%d (%s) write=%d (%s) - CACHE read=%s write=%s - toDispose=%d", pagesRead,
        PFileUtils.getSizeAsString(pagesReadSize), pagesWritten, PFileUtils.getSizeAsString(pagesWrittenSize),
        PFileUtils.getSizeAsString(readRAM), PFileUtils.getSizeAsString(writeRAM), pagesToDispose));

    buffer.append(String.format("\n FileManager openFiles=%d - maxFilesOpened=%d", totalOpenFiles, maxOpenFiles));

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
