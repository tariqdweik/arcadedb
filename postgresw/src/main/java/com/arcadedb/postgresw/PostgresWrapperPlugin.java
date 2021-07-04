/*
 * Copyright (c) - Arcade Data LTD (https://arcadedata.com)
 */

package com.arcadedb.postgresw;

import com.arcadedb.ContextConfiguration;
import com.arcadedb.server.ArcadeDBServer;
import com.arcadedb.server.ServerPlugin;
import com.arcadedb.server.ha.network.DefaultServerSocketFactory;

public class PostgresWrapperPlugin implements ServerPlugin {
  private              ArcadeDBServer       server;
  private              ContextConfiguration configuration;
  private final static int                  DEF_PORT = 5432;
  private              PostgresNetworkListener listener;

  @Override
  public void configure(final ArcadeDBServer arcadeDBServer, final ContextConfiguration configuration) {
    this.server = arcadeDBServer;
    this.configuration = configuration;
  }

  @Override
  public void startService() {
    listener = new PostgresNetworkListener(server, new DefaultServerSocketFactory(), "localhost", "" + DEF_PORT);
  }

  @Override
  public void stopService() {
    if (listener != null)
      listener.close();
  }
}