/*
 * Copyright (c) 2018 - Arcade Analytics LTD (https://arcadeanalytics.com)
 */
package com.arcadedb.server.ha.message;

import com.arcadedb.database.Binary;
import com.arcadedb.server.ha.HAServer;

public class ReplicaReadyRequest implements HACommand {

  public ReplicaReadyRequest() {
  }

  @Override
  public HACommand execute(final HAServer server, final String remoteServerName) {
    server.setReplicaStatus(remoteServerName, true);
    return null;
  }

  @Override
  public void toStream(final Binary stream) {
  }

  @Override
  public void fromStream(final Binary stream) {
  }

  @Override
  public String toString() {
    return "replicaOnline";
  }
}
