/*
 * Copyright (c) 2018 - Arcade Analytics LTD (https://arcadeanalytics.com)
 */

package com.arcadedb.log;

import java.util.logging.Level;

/**
 * Centralized Log Manager.
 *
 * @author Luca Garulli
 */
public class LogManager {
  private static final LogManager instance = new LogManager();
  private              boolean    debug    = false;
  private              boolean    info     = true;
  private              boolean    warn     = true;
  private              boolean    error    = true;
  private              Logger     logger   = new DefaultLogger();

  static class LogContext extends ThreadLocal<String> {
  }

  public static LogContext CONTEXT_INSTANCE = new LogContext();

  protected LogManager() {
  }

  public static LogManager instance() {
    return instance;
  }

  public String getContext() {
    return CONTEXT_INSTANCE.get();
  }

  public void setContext(final String context) {
    CONTEXT_INSTANCE.set(context);
  }

  public Logger getLogger() {
    return logger;
  }

  public void setLogger(final Logger logger) {
    this.logger = logger;
  }

  public void log(final Object iRequester, final Level iLevel, String iMessage, final Throwable iException, boolean extractDBData,
      final Object... iAdditionalArgs) {
    logger.log(iRequester, iLevel, iMessage, iException, extractDBData, CONTEXT_INSTANCE.get(), iAdditionalArgs);
  }

  public void debug(final Object iRequester, final String iMessage, final Object... iAdditionalArgs) {
    log(iRequester, Level.FINE, iMessage, null, true, iAdditionalArgs);
  }

  public void debug(final Object iRequester, final String iMessage, final Throwable iException, final Object... iAdditionalArgs) {
    log(iRequester, Level.FINE, iMessage, iException, true, iAdditionalArgs);
  }

  public void info(final Object iRequester, final String iMessage, final Object... iAdditionalArgs) {
    log(iRequester, Level.INFO, iMessage, null, true, iAdditionalArgs);
  }

  public void info(final Object iRequester, final String iMessage, final Throwable iException, final Object... iAdditionalArgs) {
    log(iRequester, Level.INFO, iMessage, iException, true, iAdditionalArgs);
  }

  public void warn(final Object iRequester, final String iMessage, final Object... iAdditionalArgs) {
    log(iRequester, Level.WARNING, iMessage, null, true, iAdditionalArgs);
  }

  public void warn(final Object iRequester, final String iMessage, final Throwable iException, final Object... iAdditionalArgs) {
    log(iRequester, Level.WARNING, iMessage, iException, true, iAdditionalArgs);
  }

  public void config(final Object iRequester, final String iMessage, final Object... iAdditionalArgs) {
    log(iRequester, Level.CONFIG, iMessage, null, true, iAdditionalArgs);
  }

  public void error(final Object iRequester, final String iMessage, final Throwable iException, final Object... iAdditionalArgs) {
    if (isErrorEnabled())
      log(iRequester, Level.SEVERE, iMessage, iException, true, iAdditionalArgs);
  }

  public void errorNoDb(final Object iRequester, final String iMessage, final Throwable iException, final Object... iAdditionalArgs) {
    if (isErrorEnabled())
      log(iRequester, Level.SEVERE, iMessage, iException, false, iAdditionalArgs);
  }

  public boolean isWarn() {
    return warn;
  }

  public boolean isLevelEnabled(final Level level) {
    if (level.equals(Level.FINER) || level.equals(Level.FINE) || level.equals(Level.FINEST))
      return debug;
    else if (level.equals(Level.INFO))
      return info;
    else if (level.equals(Level.WARNING))
      return warn;
    else if (level.equals(Level.SEVERE))
      return error;
    return false;
  }

  public boolean isDebugEnabled() {
    return debug;
  }

  public void setDebugEnabled(boolean debug) {
    this.debug = debug;
  }

  public boolean isInfoEnabled() {
    return info;
  }

  public void setInfoEnabled(boolean info) {
    this.info = info;
  }

  public boolean isWarnEnabled() {
    return warn;
  }

  public void setWarnEnabled(boolean warn) {
    this.warn = warn;
  }

  public boolean isErrorEnabled() {
    return error;
  }

  public void setErrorEnabled(boolean error) {
    this.error = error;
  }

  protected void setLevelInternal(final Level level) {
    if (level == null)
      return;

    if (level.equals(Level.FINER) || level.equals(Level.FINE) || level.equals(Level.FINEST))
      debug = info = warn = error = true;
    else if (level.equals(Level.INFO)) {
      info = warn = error = true;
      debug = false;
    } else if (level.equals(Level.WARNING)) {
      warn = error = true;
      debug = info = false;
    } else if (level.equals(Level.SEVERE)) {
      error = true;
      debug = info = warn = false;
    }
  }

  public void flush() {
    logger.flush();
  }
}
