package com.arcadedb;

import com.arcadedb.utility.PCallable;
import com.arcadedb.utility.PFileUtils;
import com.arcadedb.utility.PLogManager;

import java.io.PrintStream;
import java.util.Locale;
import java.util.Map;
import java.util.Timer;
import java.util.TimerTask;

/**
 * Keeps all configuration settings. At startup assigns the configuration values by reading system properties.
 */
public enum PGlobalConfiguration {
  // ENVIRONMENT
  DUMP_CONFIG_AT_STARTUP("proton.dumpConfigAtStartup", "Dumps the configuration at startup", Boolean.class, false,
      new PCallable<Object, Object>() {
        @Override
        public Object call(final Object value) {
          dumpConfiguration(System.out);
          return value;
        }
      }),

  DUMP_METRICS_EVERY("proton.dumpMetricsEvery", "Dumps the metrics at startup, shutdown and every configurable amount of time",
      Long.class, 0, new PCallable<Object, Object>() {
    @Override
    public Object call(final Object value) {
      final long time = (long) value;
      if (time > 0) {
        PProfiler.INSTANCE.dumpMetrics(System.out);

        TIMER.schedule(new TimerTask() {
          @Override
          public void run() {
            PProfiler.INSTANCE.dumpMetrics(System.out);
          }
        }, time, time);
      }
      return value;
    }
  }),

  MAX_PAGE_RAM("proton.maxPageRAM", "Maximum amount of pages (in MB) to keep in RAM", Long.class, 4 * 1024l * 1024l,
      new PCallable<Object, Object>() {
        @Override
        public Object call(final Object value) {
          final long maxRAM = (long) value;
          if (maxRAM > Runtime.getRuntime().maxMemory() * 80 / 100) {
            final long newValue = Runtime.getRuntime().maxMemory() / 2 / 1000;
            PLogManager.instance()
                .warn(this, "Setting '%s' is > than 80% of maximum heap (%s). Decreasing it to %s", MAX_PAGE_RAM.key,
                    PFileUtils.getSizeAsString(Runtime.getRuntime().maxMemory()), PFileUtils.getSizeAsString(newValue));
            return newValue;
          }
          return value;
        }
      }, new PCallable<Object, Object>() {
    @Override
    public Object call(final Object value) {
      return Runtime.getRuntime().maxMemory() / 2 / 1000;
    }
  }),

  FLUSH_ONLY_AT_CLOSE("proton.flushOnlyAtClose", "Never flushes pages on disk until the close of database", Boolean.class, false),

  FREE_PAGE_RAM("proton.freePageRAM", "Percentage (0-100) of memory to free when Page RAM is full", Integer.class, 50),

  COMMIT_LOCK_TIMEOUT("proton.commitLockTimeout", "Timeout in ms to lock resources during commit", Long.class, 5000),

  MVCC_RETRIES("proton.mvccRetries", "Number of retries in case of MVCC exception", Integer.class, 50),

  INDEX_COMPACTION_RAM("proton.indexCompactionRAM", "Maximum amount of RAM to use for index compaction, in MB", Long.class, 300);

  /**
   * Place holder for the "undefined" value of setting.
   */
  private final Object nullValue = new Object();

  private final String                    key;
  private final Object                    defValue;
  private final Class<?>                  type;
  private final PCallable<Object, Object> callback;
  private final PCallable<Object, Object> callbackIfNoSet;
  private volatile Object value = nullValue;
  private final String  description;
  private final Boolean canChangeAtRuntime;
  private final boolean hidden;

  private static final Timer TIMER;

  static {
    TIMER = new Timer(true);
    readConfiguration();
  }

  PGlobalConfiguration(final String iKey, final String iDescription, final Class<?> iType, final Object iDefValue) {
    this(iKey, iDescription, iType, iDefValue, null);
  }

  PGlobalConfiguration(final String iKey, final String iDescription, final Class<?> iType, final Object iDefValue,
      final PCallable<Object, Object> callback) {
    this.key = iKey;
    this.description = iDescription;
    this.defValue = iDefValue;
    this.type = iType;
    this.canChangeAtRuntime = true;
    this.hidden = false;
    this.callback = callback;
    this.callbackIfNoSet = null;
  }

  PGlobalConfiguration(final String iKey, final String iDescription, final Class<?> iType, final Object iDefValue,
      final PCallable<Object, Object> callback, final PCallable<Object, Object> callbackIfNoSet) {
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
    out.print("PROTON ");
    out.print(PConstants.getVersion());
    out.println(" configuration:");

    String lastSection = "";
    for (PGlobalConfiguration v : values()) {
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
  public static PGlobalConfiguration findByKey(final String iKey) {
    for (PGlobalConfiguration v : values()) {
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
      for (PGlobalConfiguration v : values()) {
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

    for (PGlobalConfiguration config : values()) {
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
        PLogManager.instance().error(this, "Error during setting property %s=%s", e, key, value);
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
    return (int) (v instanceof Number ? ((Number) v).intValue() : PFileUtils.getSizeAsNumber(v.toString()));
  }

  public long getValueAsLong() {
    final Object v = value != nullValue && value != null ? value : defValue;
    return v instanceof Number ? ((Number) v).longValue() : PFileUtils.getSizeAsNumber(v.toString());
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
