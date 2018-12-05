/*
 * Copyright (c) 2018 - Arcade Analytics LTD (https://arcadeanalytics.com)
 */

package com.arcadedb.log;

import java.util.logging.Level;

/**
 * Logger interface that avoids using varargs to remove garbage on the GC
 */
public interface Logger {
  void log(final Object iRequester, final Level iLevel, String iMessage, final Throwable iException, final String context,
      final Object arg1, final Object arg2, final Object arg3, final Object arg4, final Object arg5, final Object arg6, final Object arg7,
      final Object arg8, final Object arg9, final Object arg10, Object arg11, Object arg12);

  void flush();
}