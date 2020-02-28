/*
 * Copyright (c) - Arcade Analytics LTD (https://arcadeanalytics.com)
 */

package com.arcadedb.utility;

import com.arcadedb.log.LogManager;

import java.io.*;
import java.lang.management.ManagementFactory;
import java.lang.management.ThreadInfo;
import java.lang.management.ThreadMXBean;
import java.nio.channels.FileChannel;
import java.nio.file.FileSystem;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Locale;
import java.util.logging.Level;

public class JVMUtils {
  public static String generateThreadDump(final String filter) {
    final StringBuilder output = new StringBuilder();
    final ThreadMXBean threadMXBean = ManagementFactory.getThreadMXBean();
    final ThreadInfo[] threadInfos = threadMXBean.getThreadInfo(threadMXBean.getAllThreadIds(), 100);
    for (ThreadInfo threadInfo : threadInfos) {
      if (filter != null) {
        boolean found = false;
        final StackTraceElement[] stackTraceElements = threadInfo.getStackTrace();
        for (final StackTraceElement stackTraceElement : stackTraceElements) {
          if (stackTraceElement.toString().contains(filter)) {
            found = true;
            break;
          }
        }

        if (!found)
          continue;
      }

      output.append('"');
      output.append(threadInfo.getThreadName());
      output.append("\" ");
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
