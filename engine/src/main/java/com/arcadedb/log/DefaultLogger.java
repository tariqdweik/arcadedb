/*
 * Copyright (c) 2018 - Arcade Analytics LTD (https://arcadeanalytics.com)
 */

package com.arcadedb.log;

import com.arcadedb.utility.AnsiLogFormatter;
import com.arcadedb.utility.SystemVariableResolver;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.logging.ConsoleHandler;
import java.util.logging.Handler;
import java.util.logging.Level;

/**
 * Default Logger implementation that writes to the Java Logging Framework.
 * Set the property `java.util.logging.config.file` to the configuration file to use.
 */
public class DefaultLogger implements Logger {
  private static final String                                          DEFAULT_LOG                  = "com.arcadedb";
  private static final String                                          ENV_INSTALL_CUSTOM_FORMATTER = "arcadedb.installCustomFormatter";
  private static final DefaultLogger                                   instance                     = new DefaultLogger();
  private final        ConcurrentMap<String, java.util.logging.Logger> loggersCache                 = new ConcurrentHashMap<String, java.util.logging.Logger>();

  public DefaultLogger() {
    installCustomFormatter();
  }

  public static DefaultLogger instance() {
    return instance;
  }

  public void installCustomFormatter() {
    final boolean installCustomFormatter = Boolean
        .parseBoolean(SystemVariableResolver.resolveSystemVariables("${" + ENV_INSTALL_CUSTOM_FORMATTER + "}", "true"));

    if (!installCustomFormatter)
      return;

    try {
      // ASSURE TO HAVE THE LOG FORMATTER TO THE CONSOLE EVEN IF NO CONFIGURATION FILE IS TAKEN
      final java.util.logging.Logger log = java.util.logging.Logger.getLogger("");

      if (log.getHandlers().length == 0) {
        // SET DEFAULT LOG FORMATTER
        final Handler h = new ConsoleHandler();
        h.setFormatter(new AnsiLogFormatter());
        log.addHandler(h);
      } else {
        for (Handler h : log.getHandlers()) {
          if (h instanceof ConsoleHandler && !h.getFormatter().getClass().equals(AnsiLogFormatter.class))
            h.setFormatter(new AnsiLogFormatter());
        }
      }
    } catch (Exception e) {
      System.err.println("Error while installing custom formatter. Logging could be disabled. Cause: " + e.toString());
    }
  }

  public void log(final Object iRequester, final Level iLevel, String iMessage, final Throwable iException, final boolean extractDBData, final String context,
      final Object... iAdditionalArgs) {
    if (iMessage != null) {
      final String requesterName;
      if (iRequester instanceof Class<?>) {
        requesterName = ((Class<?>) iRequester).getName();
      } else if (iRequester != null) {
        requesterName = iRequester.getClass().getName();
      } else {
        requesterName = DEFAULT_LOG;
      }

      java.util.logging.Logger log = loggersCache.get(requesterName);
      if (log == null) {
        log = java.util.logging.Logger.getLogger(requesterName);

        if (log != null) {
          java.util.logging.Logger oldLogger = loggersCache.putIfAbsent(requesterName, log);

          if (oldLogger != null)
            log = oldLogger;
        }
      }

      if (log == null) {
        if (context != null)
          iMessage = "<" + context + "> " + iMessage;

        // USE SYSERR
        try {
          System.err.println(String.format(iMessage, iAdditionalArgs));
        } catch (Exception e) {
          System.err.print(String.format("Error on formatting message '%s'. Exception: %s", iMessage, e.toString()));
        }
      } else if (log.isLoggable(iLevel)) {
        // USE THE LOG
        try {
          if (context != null)
            iMessage = "<" + context + "> " + iMessage;

          final String msg = String.format(iMessage, iAdditionalArgs);
          if (iException != null)
            log.log(iLevel, msg, iException);
          else
            log.log(iLevel, msg);
        } catch (Exception e) {
          System.err.print(String.format("Error on formatting message '%s'. Exception: %s", iMessage, e.toString()));
        }
      }
    }
  }

  @Override
  public void flush() {
    for (Handler h : java.util.logging.Logger.getLogger(java.util.logging.Logger.GLOBAL_LOGGER_NAME).getHandlers())
      h.flush();
  }
}
