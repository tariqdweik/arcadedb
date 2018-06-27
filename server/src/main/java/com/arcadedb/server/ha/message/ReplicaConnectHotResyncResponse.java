/*
 * Copyright (c) 2018 - Arcade Analytics LTD (https://arcadeanalytics.com)
 */
package com.arcadedb.server.ha.message;

import com.arcadedb.server.ha.HAServer;

public class ReplicaConnectHotResyncResponse extends HAAbstractCommand {

  public ReplicaConnectHotResyncResponse() {
  }

  @Override
  public HACommand execute(final HAServer server, final String remoteServerName, final long messageNumber) {
    return null;
  }

  @Override
  public String toString() {
    return "hotResync";
  }
}
