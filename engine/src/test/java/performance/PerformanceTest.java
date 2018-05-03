package performance;

import com.arcadedb.utility.PFileUtils;

import java.io.File;

public abstract class PerformanceTest {
  public final static String DATABASE_PATH = "target/database/performance";

  public static void clean() {
    final File dir = new File(PerformanceTest.DATABASE_PATH);
    PFileUtils.deleteRecursively(dir);
    dir.mkdirs();
  }
}