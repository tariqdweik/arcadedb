/*
 * Copyright (c) - Arcade Data LTD (https://arcadedata.com)
 */

package com.arcadedb.utility;

import java.lang.management.ManagementFactory;
import java.lang.management.ThreadInfo;
import java.lang.management.ThreadMXBean;

public class JVMUtils {
  public static String generateThreadDump(String filterInclude, String filterExclude) {
    if (filterInclude != null && filterInclude.trim().isEmpty())
      filterInclude = null;

    if (filterExclude != null && filterExclude.trim().isEmpty())
      filterExclude = null;

    final StringBuilder output = new StringBuilder();
    final ThreadMXBean threadMXBean = ManagementFactory.getThreadMXBean();
    final ThreadInfo[] threadInfos = threadMXBean.getThreadInfo(threadMXBean.getAllThreadIds(), 100);
    for (ThreadInfo threadInfo : threadInfos) {
      if (threadInfo == null)
        continue;

      if (filterInclude != null || filterExclude != null) {
        boolean found = false;
        final StackTraceElement[] stackTraceElements = threadInfo.getStackTrace();
        for (final StackTraceElement stackTraceElement : stackTraceElements) {
          if (filterInclude != null && stackTraceElement.toString().contains(filterInclude)) {
            found = true;
            break;
          }

          if (filterExclude != null && stackTraceElement.toString().contains(filterExclude)) {
            found = false;
            break;
          }
        }

        if (!found)
          continue;
      }

      output.append('"');
      output.append(threadInfo.getThreadName());
      output.append("\" ");

      output.append(String.format("\nWaited %d times = %dms - Blocked %d times = %dms - Locked monitors=%d synchronizers=%d - InNative=%s",//
          threadInfo.getWaitedCount(), threadInfo.getWaitedTime(), threadInfo.getBlockedCount(), threadInfo.getBlockedTime(),//
          threadInfo.getLockedMonitors().length, threadInfo.getLockedSynchronizers().length, threadInfo.isInNative()));

      if (threadInfo.getLockInfo() != null) {
        output.append(String.format("\nWaiting for lock %s", threadInfo.getLockName()));
        if (threadInfo.getLockOwnerName() != null)
          output.append(String.format(" owned by %s(%s)", threadInfo.getLockOwnerName(), threadInfo.getLockOwnerId()));
      }

      final Thread.State state = threadInfo.getThreadState();
      output.append("\n   java.lang.Thread.State: ");
      output.append(state);

      final StackTraceElement[] stackTraceElements = threadInfo.getStackTrace();
      for (final StackTraceElement stackTraceElement : stackTraceElements) {
        output.append("\n        at ");
        output.append(stackTraceElement);
      }
      output.append("\n\n");
    }
    return output.toString();
  }
}
