/*
 * Copyright (c) 2018 - Arcade Analytics LTD (https://arcadeanalytics.com)
 */

package com.arcadedb.log;

import java.util.logging.Level;

public interface Logger {
  void log(final Object iRequester, final Level iLevel, String iMessage, final Throwable iException, boolean extractDBData, final String context,
      final Object... iAdditionalArgs);

  void flush();
}