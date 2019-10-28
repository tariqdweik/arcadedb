/*
 * Copyright (c) - Arcade Analytics LTD (https://arcadeanalytics.com)
 */

package com.arcadedb.server;

import com.arcadedb.ContextConfiguration;

public interface ServerPlugin {
  void configure(ArcadeDBServer arcadeDBServer, ContextConfiguration configuration);

  void startService();

  void stopService();
}
