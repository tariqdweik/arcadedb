/*
 * Copyright (c) 2018 - Arcade Analytics LTD (https://arcadeanalytics.com)
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
  protected final DatabaseFactory factory = new DatabaseFactory(getDatabasePath());
  protected       Database        database;

  protected BaseTest() {
    FileUtils.deleteRecursively(new File(getDatabasePath()));
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
      database.drop();
      database = null;
    }
    FileUtils.deleteRecursively(new File(getDatabasePath()));
  }
}