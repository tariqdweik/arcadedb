/*
 * Copyright (c) 2018 - Arcade Analytics LTD (https://arcadeanalytics.com)
 */
package com.arcadedb.server.ha.message;

import com.arcadedb.server.ha.HAServer;
import com.arcadedb.server.ha.Leader2ReplicaNetworkExecutor;

public class ReplicaConnectRequest extends HAAbstractCommand {
  @Override
  public HACommand execute(final HAServer server, final String remoteServerName, final long messageNumber) {
    final Leader2ReplicaNetworkExecutor replica = server.getReplica(remoteServerName);
    if (replica != null && replica.isHotResyncPossible())
      return new ReplicaConnectHotResyncResponse();

    return new ReplicaConnectFullResyncResponse(server.getServer().getDatabaseNames());
  }

  @Override
  public String toString() {
    return "dbs()";
  }
}
