/*
 * Copyright (c) - Arcade Data LTD (https://arcadedata.com)
 */

package com.arcadedb.server;

import com.arcadedb.ContextConfiguration;

public interface ServerPlugin {
  void configure(ArcadeDBServer arcadeDBServer, ContextConfiguration configuration);

  void startService();

  void stopService();
}
