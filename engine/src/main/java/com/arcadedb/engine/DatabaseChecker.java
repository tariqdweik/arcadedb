package com.arcadedb.engine;

import com.arcadedb.database.Database;
import com.arcadedb.utility.LogManager;

import java.util.Map;

public class DatabaseChecker {
  public void check(Database database) {
    LogManager.instance().info(this, "Starting checking database '%s'...", database.getName());

    long autofix = 0;
    long warnings = 0;
    long errors = 0;

    long pageSize = 0;
    long totalPages = 0;
    long totalRecords = 0;
    long totalActiveRecords = 0;
    long totalPlaceholderRecords = 0;
    long totalSurrogateRecords = 0;
    long totalDeletedRecords = 0;
    long totalMaxOffset = 0;

    for (Bucket b : database.getSchema().getBuckets()) {
      final Map<String, Long> stats = b.check();

      pageSize += stats.get("pageSize");
      totalPages += stats.get("totalPages");
      totalRecords += stats.get("totalRecords");
      totalActiveRecords += stats.get("totalActiveRecords");
      totalPlaceholderRecords += stats.get("totalPlaceholderRecords");
      totalSurrogateRecords += stats.get("totalSurrogateRecords");
      totalDeletedRecords += stats.get("totalDeletedRecords");
      totalMaxOffset += stats.get("totalMaxOffset");
    }

    final float avgPageUsed = totalPages > 0 ? (float) (totalMaxOffset / totalPages) * 100f / pageSize : 0;

    LogManager.instance()
        .info(this, "Total records=%d (actives=%d deleted=%d placeholders=%d surrogates=%d) avgPageUsed=%.2f%%", totalRecords,
            totalActiveRecords, totalDeletedRecords, totalPlaceholderRecords, totalSurrogateRecords, avgPageUsed);

    LogManager.instance().info(this, "Completed checking of database '%s':", database.getName());
    LogManager.instance().info(this, "- warning %d", warnings);
    LogManager.instance().info(this, "- auto-fix %d", autofix);
    LogManager.instance().info(this, "- errors %d", errors);
  }
}
