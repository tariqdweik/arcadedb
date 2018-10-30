/*
 * Copyright (c) 2018 - Arcade Analytics LTD (https://arcadeanalytics.com)
 */

package performance;

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.log.LogManager;
import com.arcadedb.log.Logger;
import com.arcadedb.utility.FileUtils;

import java.io.File;
import java.util.logging.Level;

public abstract class PerformanceTest {
  public final static String DATABASE_PATH = "target/database/performance";

  public static void clean() {
    GlobalConfiguration.PROFILE.setValue("high-performance");

    LogManager.instance().setLogger(new Logger() {
      @Override
      public void log(Object iRequester, Level iLevel, String iMessage, Throwable iException, boolean extractDBData, String context,
          Object... iAdditionalArgs) {
      }

      @Override
      public void flush() {
      }
    });

    final File dir = new File(PerformanceTest.DATABASE_PATH);
    FileUtils.deleteRecursively(dir);
    dir.mkdirs();
  }
}