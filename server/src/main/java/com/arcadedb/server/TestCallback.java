/*
 * Copyright (c) 2018 - Arcade Analytics LTD (https://arcadeanalytics.com)
 */

package com.arcadedb.server;

public interface TestCallback {
  enum TYPE {SERVER_STARTING, SERVER_UP, SERVER_SHUTTING_DOWN, SERVER_DOWN, REPLICA_MSG_RECEIVED, REPLICA_ONLINE, REPLICA_OFFLINE, REPLICA_HOT_RESYNC, REPLICA_FULL_RESYNC, NETWORK_CONNECTION}

  void onEvent(TYPE type, Object object, ArcadeDBServer server) throws Exception;
}
