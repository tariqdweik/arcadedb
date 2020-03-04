/*
 * Copyright (c) - Arcade Data LTD (https://arcadedata.com)
 */

package com.arcadedb;

import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.engine.PaginatedFile;
import com.arcadedb.utility.FileUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;

import java.io.File;

public abstract class BaseTest {
  protected final DatabaseFactory factory;
  protected       Database        database;

  protected BaseTest() {
    GlobalConfiguration.PROFILE.setValue(getPerformanceProfile());

    FileUtils.deleteRecursively(new File(getDatabasePath()));

    factory = new DatabaseFactory(getDatabasePath());
    database = factory.create();
  }

  protected void reopenDatabase() {
    if (database != null)
      database.close();
    database = factory.open();
  }

  protected void reopenDatabaseInReadOnlyMode() {
    if (database != null)
      database.close();
    database = factory.open(PaginatedFile.MODE.READ_ONLY);
  }

  protected String getDatabasePath() {
    return "target/database/" + getClass().getSimpleName();
  }

  protected void beginTest() {
  }

  protected void endTest() {
  }

  @BeforeEach
  public void beforeTest() {
    beginTest();
  }

  @AfterEach
  public void afterTest() {
    endTest();
    if (database != null && database.isOpen()) {
      if (database.getMode() == PaginatedFile.MODE.READ_ONLY)
        reopenDatabase();

      database.drop();
      database = null;
    }
    FileUtils.deleteRecursively(new File(getDatabasePath()));
  }

  protected String getPerformanceProfile() {
    return "default";
  }
}