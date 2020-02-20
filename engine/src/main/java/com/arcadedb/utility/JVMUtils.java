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
  public static String generateThreadDump() {
    final StringBuilder dump = new StringBuilder();
    final ThreadMXBean threadMXBean = ManagementFactory.getThreadMXBean();
    final ThreadInfo[] threadInfos = threadMXBean.getThreadInfo(threadMXBean.getAllThreadIds(), 100);
    for (ThreadInfo threadInfo : threadInfos) {
      dump.append('"');
      dump.append(threadInfo.getThreadName());
      dump.append("\" ");
      final Thread.State state = threadInfo.getThreadState();
      dump.append("\n   java.lang.Thread.State: ");
      dump.append(state);
      final StackTraceElement[] stackTraceElements = threadInfo.getStackTrace();
      for (final StackTraceElement stackTraceElement : stackTraceElements) {
        dump.append("\n        at ");
        dump.append(stackTraceElement);
      }
      dump.append("\n\n");
    }
    return dump.toString();
  }
}
