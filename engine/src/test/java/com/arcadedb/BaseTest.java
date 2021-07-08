/*
 * Copyright 2021 Arcade Data Ltd
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
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
    this(true);
  }

  protected BaseTest(final boolean cleanBeforeTest) {
    GlobalConfiguration.PROFILE.setValue(getPerformanceProfile());

    if (cleanBeforeTest)
      FileUtils.deleteRecursively(new File(getDatabasePath()));
    factory = new DatabaseFactory(getDatabasePath());
    database = factory.exists() ? factory.open() : factory.create();
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