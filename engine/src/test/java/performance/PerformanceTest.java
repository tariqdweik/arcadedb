/*
 * Copyright (c) 2018 - Arcade Analytics LTD (https://arcadeanalytics.com)
 */

package performance;

import com.arcadedb.utility.FileUtils;

import java.io.File;

public abstract class PerformanceTest {
  public final static String DATABASE_PATH = "target/database/performance";

  public static void clean() {
    final File dir = new File(PerformanceTest.DATABASE_PATH);
    FileUtils.deleteRecursively(dir);
    dir.mkdirs();
  }
}