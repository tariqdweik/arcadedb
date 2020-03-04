/*
 * Copyright (c) - Arcade Data LTD (https://arcadedata.com)
 */
package com.arcadedb.server.ha.message;

import com.arcadedb.server.ha.HAServer;

public class ReplicaReadyRequest extends HAAbstractCommand {

  @Override
  public HACommand execute(final HAServer server, final String remoteServerName, final long messageNumber) {
    server.setReplicaStatus(remoteServerName, true);
    return null;
  }

  @Override
  public String toString() {
    return "replicaOnline";
  }
}
