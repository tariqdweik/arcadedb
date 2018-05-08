package com.arcadedb.utility;

import java.util.Locale;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.logging.*;

/**
 * Centralized Log Manager.
 * <p>
 * Set the property `java.util.logging.config.file` to the configuration file to use.
 *
 * @author Luca Garulli
 */
public class LogManager {
  private static final String     DEFAULT_LOG                  = "com.arcadedb";
  private static final String     ENV_INSTALL_CUSTOM_FORMATTER = "arcadedb.installCustomFormatter";
  private static final LogManager instance                     = new LogManager();
  private              boolean    debug                        = false;
  private              boolean    info                         = true;
  private              boolean    warn                         = true;
  private              boolean    error                        = true;
  private              Level      minimumLevel                 = Level.SEVERE;

  private final ConcurrentMap<String, Logger> loggersCache = new ConcurrentHashMap<String, Logger>();

  protected LogManager() {
    installCustomFormatter();
  }

  public static LogManager instance() {
    return instance;
  }

  public void installCustomFormatter() {
    final boolean installCustomFormatter = Boolean
        .parseBoolean(SystemVariableResolver.resolveSystemVariables("${" + ENV_INSTALL_CUSTOM_FORMATTER + "}", "true"));

    if (!installCustomFormatter)
      return;

    try {
      // ASSURE TO HAVE THE LOG FORMATTER TO THE CONSOLE EVEN IF NO CONFIGURATION FILE IS TAKEN
      final Logger log = Logger.getLogger("");

      setLevelInternal(log.getLevel());

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

  public void setConsoleLevel(final String iLevel) {
    setLevel(iLevel, ConsoleHandler.class);
  }

  public void setFileLevel(final String iLevel) {
    setLevel(iLevel, FileHandler.class);
  }

  public void log(final Object iRequester, final Level iLevel, String iMessage, final Throwable iException, boolean extractDBData,
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

      Logger log = loggersCache.get(requesterName);
      if (log == null) {
        log = Logger.getLogger(requesterName);

        if (log != null) {
          Logger oldLogger = loggersCache.putIfAbsent(requesterName, log);

          if (oldLogger != null)
            log = oldLogger;
        }
      }

      if (log == null) {
        // USE SYSERR
        try {
          System.err.println(String.format(iMessage, iAdditionalArgs));
        } catch (Exception e) {
          System.err.print(String.format("Error on formatting message '%s'. Exception: %s", iMessage, e.toString()));
        }
      } else if (log.isLoggable(iLevel)) {
        // USE THE LOG
        try {
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

  public void errorNoDb(final Object iRequester, final String iMessage, final Throwable iException,
      final Object... iAdditionalArgs) {
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

  public Level setLevel(final String iLevel, final Class<? extends Handler> iHandler) {
    final Level level = iLevel != null ? Level.parse(iLevel.toUpperCase(Locale.ENGLISH)) : Level.INFO;

    if (level.intValue() < minimumLevel.intValue()) {
      // UPDATE MINIMUM LEVEL
      minimumLevel = level;

      setLevelInternal(level);
    }

    Logger log = Logger.getLogger(DEFAULT_LOG);
    while (log != null) {

      for (Handler h : log.getHandlers()) {
        if (h.getClass().isAssignableFrom(iHandler)) {
          h.setLevel(level);
          break;
        }
      }

      log = log.getParent();
    }

    return level;
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
    for (Handler h : Logger.getLogger(Logger.GLOBAL_LOGGER_NAME).getHandlers())
      h.flush();
  }
}
