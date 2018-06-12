/*
 * Copyright (c) 2018 - Arcade Analytics LTD (https://arcadeanalytics.com)
 */

package com.arcadedb;

import com.arcadedb.utility.Callable;
import com.arcadedb.utility.FileUtils;
import com.arcadedb.utility.LogManager;

import java.io.PrintStream;
import java.util.Locale;
import java.util.Map;
import java.util.Timer;
import java.util.TimerTask;

/**
 * Keeps all configuration settings. At startup assigns the configuration values by reading system properties.
 */
public enum GlobalConfiguration {
  // ENVIRONMENT
  DUMP_CONFIG_AT_STARTUP("arcadedb.dumpConfigAtStartup", "Dumps the configuration at startup", Boolean.class, false,
      new Callable<Object, Object>() {
        @Override
        public Object call(final Object value) {
          dumpConfiguration(System.out);
          return value;
        }
      }),

  DUMP_METRICS_EVERY("arcadedb.dumpMetricsEvery", "Dumps the metrics at startup, shutdown and every configurable amount of time",
      Long.class, 0, new Callable<Object, Object>() {
    @Override
    public Object call(final Object value) {
      final long time = (long) value;
      if (time > 0) {
        Profiler.INSTANCE.dumpMetrics(System.out);

        TIMER.schedule(new TimerTask() {
          @Override
          public void run() {
            Profiler.INSTANCE.dumpMetrics(System.out);
          }
        }, time, time);
      }
      return value;
    }
  }),

  MAX_PAGE_RAM("arcadedb.maxPageRAM", "Maximum amount of pages (in MB) to keep in RAM", Long.class, 4 * 1024l * 1024l,
      new Callable<Object, Object>() {
        @Override
        public Object call(final Object value) {
          final long maxRAM = (long) value;
          if (maxRAM > Runtime.getRuntime().maxMemory() * 80 / 100) {
            final long newValue = Runtime.getRuntime().maxMemory() / 2 / 1000;
            LogManager.instance()
                .warn(this, "Setting '%s' is > than 80% of maximum heap (%s). Decreasing it to %s", MAX_PAGE_RAM.key,
                    FileUtils.getSizeAsString(Runtime.getRuntime().maxMemory()), FileUtils.getSizeAsString(newValue));
            return newValue;
          }
          return value;
        }
      }, new Callable<Object, Object>() {
    @Override
    public Object call(final Object value) {
      return Runtime.getRuntime().maxMemory() / 2 / 1000;
    }
  }),

  FLUSH_ONLY_AT_CLOSE("arcadedb.flushOnlyAtClose", "Never flushes pages on disk until the database closing", Boolean.class, false),

  TX_FLUSH("arcadedb.txFlush", "Flushes the pages on disk at commit time", Boolean.class, false),

  TX_WAL("arcadedb.txWAL", "Uses the WAL", Boolean.class, true),

  FREE_PAGE_RAM("arcadedb.freePageRAM", "Percentage (0-100) of memory to free when Page RAM is full", Integer.class, 50),

  ASYNC_OPERATIONS_QUEUE("arcadedb.asyncOperationsQueue",
      "Size of the total asynchronous operation queues (it is divided by the number of parallel threads in the pool)",
      Integer.class, 1024),

  ASYNC_TX_BATCH_SIZE("arcadedb.asyncTxBatchSize", "Maximum number of operations to commit in batch by async thread", Integer.class,
      1024 * 10),

  PAGE_FLUSH_QUEUE("arcadedb.pageFlushQueue", "Size of the asynchronous page flush queue", Integer.class, 128),

  COMMIT_LOCK_TIMEOUT("arcadedb.commitLockTimeout", "Timeout in ms to lock resources during commit", Long.class, 5000),

  MVCC_RETRIES("arcadedb.mvccRetries", "Number of retries in case of MVCC exception", Integer.class, 50),

  // SQL
  SQL_STATEMENT_CACHE("arcadedb.sqlStatementCache", "Maximum number of parsed statements to keep in cache", Integer.class, 300),

  // INDEXES
  INDEX_COMPACTION_RAM("arcadedb.indexCompactionRAM", "Maximum amount of RAM to use for index compaction, in MB", Long.class, 300),

  // NETWORK
  NETWORK_SOCKET_BUFFER_SIZE("arcadedb.network.socketBufferSize", "TCP/IP Socket buffer size, if 0 use the OS default",
      Integer.class, 0),

  NETWORK_SOCKET_TIMEOUT("arcadedb.network.socketTimeout", "TCP/IP Socket timeout (in ms)", Integer.class, 3000),

  NETWORK_USE_SSL("arcadedb.ssl.enabled", "Use SSL for client connections", Boolean.class, false),

  NETWORK_SSL_KEYSTORE("arcadedb.ssl.keyStore", "Use SSL for client connections", String.class, null),

  NETWORK_SSL_KEYSTORE_PASSWORD("arcadedb.ssl.keyStorePass", "Use SSL for client connections", String.class, null),

  NETWORK_SSL_TRUSTSTORE("arcadedb.ssl.trustStore", "Use SSL for client connections", String.class, null),

  NETWORK_SSL_TRUSTSTORE_PASSWORD("arcadedb.ssl.trustStorePass", "Use SSL for client connections", String.class, null),

  // SERVER
  SERVER_NAME("arcadedb.server.name", "Server name", String.class, Constants.PRODUCT + "_0"),

  SERVER_DATABASE_DIRECTORY("arcadedb.server.databaseDirectory", "Directory containing the database", String.class, "../databases"),

  // SERVER HTTP
  SERVER_HTTP_INCOMING_HOST("arcadedb.server.httpIncomingHost", "TCP/IP host name used for incoming HTTP connections", String.class,
      "localhost"),

  SERVER_HTTP_INCOMING_PORT("arcadedb.server.httpIncomingPort", "TCP/IP port number used for incoming HTTP connections",
      Integer.class, 2480),

  SERVER_HTTP_AUTOINCREMENT_PORT("arcadedb.server.httpAutoIncrementPort",
      "True to increment the TCP/IP port number used for incoming HTTP in case the configured is not available", Boolean.class,
      true),

  SERVER_SECURITY_ALGORITHM("arcadedb.server.securityAlgorithm", "Default encryption algorithm used for passwords hashing",
      String.class, "PBKDF2WithHmacSHA256"),

  SERVER_SECURITY_SALT_CACHE_SIZE("arcadedb.server.securitySaltCacheSize",
      "Cache size of hashed salt passwords. The cache works as LRU. Use 0 to disable the cache", Integer.class, 64),

  SERVER_SECURITY_SALT_ITERATIONS("arcadedb.server.saltIterations",
      "Number of iterations to generate the salt or user password. Changing this setting does not affect stored passwords",
      Integer.class, 65536),

  // HA
  HA_ENABLED("arcadedb.ha.enabled", "True if HA is enabled for the current server", Boolean.class, false),

  HA_QUORUM("arcadedb.ha.quorum", "Default quorum between none, 1, 2, 3, majority and all servers. Default is majority",
      String.class, "MAJORITY"),

  HA_QUORUM_TIMEOUT("arcadedb.ha.quorumTimeout", "Timeout waiting for the quorum", Long.class, 10000),

  HA_REPLICATION_INCOMING_HOST("arcadedb.ha.replicationIncomingHost", "TCP/IP host name used for incoming replication connections",
      String.class, "localhost"),

  HA_REPLICATION_INCOMING_PORTS("arcadedb.ha.replicationIncomingPorts",
      "TCP/IP port number used for incoming replication connections", String.class, "2424-2433"),

  HA_CLUSTER_NAME("arcadedb.ha.clusterName",
      "Cluster name. By default is 'arcadedb'. Useful in case of multiple clusters in the same network", String.class,
      Constants.PRODUCT.toLowerCase()),

  HA_SERVER_LIST("arcadedb.ha.serverList",
      "List of <hostname/ip-address:port> items separated by comma. Example: localhost:2424,192.168.0.1:2424", String.class, ""),;

  /**
   * Place holder for the "undefined" value of setting.
   */
  private final Object nullValue = new Object();

  private final    String                   key;
  private final    Object                   defValue;
  private final    Class<?>                 type;
  private final    Callable<Object, Object> callback;
  private final    Callable<Object, Object> callbackIfNoSet;
  private volatile Object                   value = nullValue;
  private final    String                   description;
  private final    Boolean                  canChangeAtRuntime;
  private final    boolean                  hidden;

  private static final Timer TIMER;

  static {
    TIMER = new Timer(true);
    readConfiguration();
  }

  GlobalConfiguration(final String iKey, final String iDescription, final Class<?> iType, final Object iDefValue) {
    this(iKey, iDescription, iType, iDefValue, null);
  }

  GlobalConfiguration(final String iKey, final String iDescription, final Class<?> iType, final Object iDefValue,
      final Callable<Object, Object> callback) {
    this.key = iKey;
    this.description = iDescription;
    this.defValue = iDefValue;
    this.type = iType;
    this.canChangeAtRuntime = true;
    this.hidden = false;
    this.callback = callback;
    this.callbackIfNoSet = null;
  }

  GlobalConfiguration(final String iKey, final String iDescription, final Class<?> iType, final Object iDefValue,
      final Callable<Object, Object> callback, final Callable<Object, Object> callbackIfNoSet) {
    this.key = iKey;
    this.description = iDescription;
    this.defValue = iDefValue;
    this.type = iType;
    this.canChangeAtRuntime = true;
    this.hidden = false;
    this.callback = callback;
    this.callbackIfNoSet = callbackIfNoSet;
  }

  public static void dumpConfiguration(final PrintStream out) {
    out.print("ARCADEDB ");
    out.print(Constants.VERSION);
    out.println(" configuration:");

    String lastSection = "";
    for (GlobalConfiguration v : values()) {
      final String section = v.key.substring(0, v.key.indexOf('.'));

      if (!lastSection.equals(section)) {
        out.print("- ");
        out.println(section.toUpperCase(Locale.ENGLISH));
        lastSection = section;
      }
      out.print("  + ");
      out.print(v.key);
      out.print(" = ");
      out.println(v.isHidden() ? "<hidden>" : String.valueOf((Object) v.getValue()));
    }
  }

  /**
   * Find the OGlobalConfiguration instance by the key. Key is case insensitive.
   *
   * @param iKey Key to find. It's case insensitive.
   *
   * @return OGlobalConfiguration instance if found, otherwise null
   */
  public static GlobalConfiguration findByKey(final String iKey) {
    for (GlobalConfiguration v : values()) {
      if (v.getKey().equalsIgnoreCase(iKey))
        return v;
    }
    return null;
  }

  /**
   * Changes the configuration values in one shot by passing a Map of values. Keys can be the Java ENUM names or the string
   * representation of configuration values
   */
  public static void setConfiguration(final Map<String, Object> iConfig) {
    for (Map.Entry<String, Object> config : iConfig.entrySet()) {
      for (GlobalConfiguration v : values()) {
        if (v.getKey().equals(config.getKey())) {
          v.setValue(config.getValue());
          break;
        } else if (v.name().equals(config.getKey())) {
          v.setValue(config.getValue());
          break;
        }
      }
    }
  }

  /**
   * Assign configuration values by reading system properties.
   */
  private static void readConfiguration() {
    String prop;

    for (GlobalConfiguration config : values()) {
      prop = System.getProperty(config.key);
      if (prop != null)
        config.setValue(prop);
      else if (config.callbackIfNoSet != null) {
        config.setValue(config.callbackIfNoSet.call(null));
      }
    }
  }

  public <T> T getValue() {
    //noinspection unchecked
    return (T) (value != nullValue && value != null ? value : defValue);
  }

  /**
   * @return <code>true</code> if configuration was changed from default value and <code>false</code> otherwise.
   */
  public boolean isChanged() {
    return value != nullValue;
  }

  /**
   * @return Value of configuration parameter stored as enumeration if such one exists.
   *
   * @throws ClassCastException       if stored value can not be casted and parsed from string to passed in enumeration class.
   * @throws IllegalArgumentException if value associated with configuration parameter is a string bug can not be converted to
   *                                  instance of passed in enumeration class.
   */
  public <T extends Enum<T>> T getValueAsEnum(Class<T> enumType) {
    final Object value = getValue();

    if (value == null)
      return null;

    if (enumType.isAssignableFrom(value.getClass())) {
      return enumType.cast(value);
    } else if (value instanceof String) {
      final String presentation = value.toString();
      return Enum.valueOf(enumType, presentation);
    } else {
      throw new ClassCastException("Value " + value + " can not be cast to enumeration " + enumType.getSimpleName());
    }
  }

  public void setValue(final Object iValue) {
    Object oldValue = value;

    if (iValue != null)
      if (type == Boolean.class)
        value = Boolean.parseBoolean(iValue.toString());
      else if (type == Integer.class)
        value = Integer.parseInt(iValue.toString());
      else if (type == Long.class)
        value = Long.parseLong(iValue.toString());
      else if (type == Float.class)
        value = Float.parseFloat(iValue.toString());
      else if (type == String.class)
        value = iValue.toString();
      else if (type.isEnum()) {
        boolean accepted = false;

        if (type.isInstance(iValue)) {
          value = iValue;
          accepted = true;
        } else if (iValue instanceof String) {
          final String string = (String) iValue;

          for (Object constant : type.getEnumConstants()) {
            final Enum<?> enumConstant = (Enum<?>) constant;

            if (enumConstant.name().equalsIgnoreCase(string)) {
              value = enumConstant;
              accepted = true;
              break;
            }
          }
        }

        if (!accepted)
          throw new IllegalArgumentException("Invalid value of `" + key + "` option");
      } else
        value = iValue;

    if (callback != null)
      try {
        final Object newValue = callback.call(value);
        if (newValue != value)
          // OVERWRITE IT
          value = newValue;
      } catch (Exception e) {
        LogManager.instance().error(this, "Error during setting property %s=%s", e, key, value);
      }
  }

  public boolean getValueAsBoolean() {
    final Object v = value != nullValue && value != null ? value : defValue;
    return v instanceof Boolean ? (Boolean) v : Boolean.parseBoolean(v.toString());
  }

  public String getValueAsString() {
    return value != nullValue && value != null ? value.toString() : defValue != null ? defValue.toString() : null;
  }

  public int getValueAsInteger() {
    final Object v = value != nullValue && value != null ? value : defValue;
    return (int) (v instanceof Number ? ((Number) v).intValue() : FileUtils.getSizeAsNumber(v.toString()));
  }

  public long getValueAsLong() {
    final Object v = value != nullValue && value != null ? value : defValue;
    return v instanceof Number ? ((Number) v).longValue() : FileUtils.getSizeAsNumber(v.toString());
  }

  public float getValueAsFloat() {
    final Object v = value != nullValue && value != null ? value : defValue;
    return v instanceof Float ? (Float) v : Float.parseFloat(v.toString());
  }

  public String getKey() {
    return key;
  }

  public Boolean isChangeableAtRuntime() {
    return canChangeAtRuntime;
  }

  public boolean isHidden() {
    return hidden;
  }

  public Object getDefValue() {
    return defValue;
  }

  public Class<?> getType() {
    return type;
  }

  public String getDescription() {
    return description;
  }
}
