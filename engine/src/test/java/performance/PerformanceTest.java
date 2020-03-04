/*
 * Copyright (c) - Arcade Data LTD (https://arcadedata.com)
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
      public void log(Object iRequester, Level iLevel, String iMessage, Throwable iException, String context, Object arg1, Object arg2, Object arg3,
          Object arg4, Object arg5, Object arg6, Object arg7, Object arg8, Object arg9, Object arg10, Object arg11, Object arg12, Object arg13, Object arg14,
          Object arg15, Object arg16, Object arg17) {
      }

      @Override
      public void log(Object iRequester, Level iLevel, String iMessage, Throwable iException, String context, Object... args) {
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