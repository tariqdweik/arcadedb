/*
 * Copyright (c) - Arcade Analytics LTD (https://arcadeanalytics.com)
 */

package com.arcadedb.server;

import com.arcadedb.ContextConfiguration;
import com.arcadedb.GlobalConfiguration;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.IOException;

public class ServerDefaultDatabasesTest extends BaseGraphServerTest {

  @Override
  protected boolean isCreateDatabases() {
    return false;
  }

  @Override
  protected boolean isPopulateDatabase() {
    return false;
  }

  protected void onServerConfiguration(final ContextConfiguration config) {
    config.setValue(GlobalConfiguration.SERVER_DEFAULT_DATABASES, "Universe[elon:musk];Amiga[Jay:Miner,Jack:Tramiel,root]");
  }

  @Test
  public void checkDefaultDatabases() throws IOException {
    getServer(0).getSecurity().authenticate("elon", "musk");
    getServer(0).getSecurity().authenticate("Jay", "Miner");
    getServer(0).getSecurity().authenticate("Jack", "Tramiel");
    getServer(0).getSecurity().authenticate("root", "root");

    Assertions.assertTrue(getServer(0).existsDatabase("Universe"));
    Assertions.assertTrue(getServer(0).existsDatabase("Amiga"));

    deleteAllDatabases();
  }
}