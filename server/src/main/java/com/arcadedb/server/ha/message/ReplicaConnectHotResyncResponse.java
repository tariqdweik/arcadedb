/*
 * Copyright (c) 2018 - Arcade Analytics LTD (https://arcadeanalytics.com)
 */
package com.arcadedb.server.ha.message;

import com.arcadedb.server.ha.HAServer;

public class ReplicaConnectHotResyncResponse extends HAAbstractCommand {
  private long positionInLog;

  public ReplicaConnectHotResyncResponse() {
  }

  public ReplicaConnectHotResyncResponse(final long positionInLog) {
    this.positionInLog = positionInLog;
  }

  public long getPositionInLog() {
    return positionInLog;
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
