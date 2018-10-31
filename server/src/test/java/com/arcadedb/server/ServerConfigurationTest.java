/*
 * Copyright (c) 2018 - Arcade Analytics LTD (https://arcadeanalytics.com)
 */

package com.arcadedb.server;

import com.arcadedb.ContextConfiguration;
import com.arcadedb.utility.FileUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;

import static com.arcadedb.GlobalConfiguration.TX_WAL;

public class ServerConfigurationTest extends BaseGraphServerTest {
  @Test
  public void testServerLoadConfiguration() throws IOException {
    final ContextConfiguration cfg = new ContextConfiguration();

    Assertions.assertTrue(cfg.getValueAsBoolean(TX_WAL));

    cfg.setValue(TX_WAL, false);

    Assertions.assertFalse(cfg.getValueAsBoolean(TX_WAL));

    final File file = new File(ArcadeDBServer.CONFIG_SERVER_CONFIGURATION_FILENAME);
    if (file.exists())
      file.delete();

    FileUtils.writeFile(file, cfg.toJSON());
    try {

      final ArcadeDBServer server = new ArcadeDBServer();
      server.start();

      Assertions.assertFalse(server.getConfiguration().getValueAsBoolean(TX_WAL));
    } finally {
      if (file.exists())
        file.delete();
    }
  }
}