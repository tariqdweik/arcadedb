/*
 * Copyright (c) - Arcade Data LTD (https://arcadedata.com)
 */
package com.arcadedb.server.ha.message;

import com.arcadedb.server.ha.HAServer;

public class ReplicaConnectHotResyncResponse extends HAAbstractCommand {
  private long messageNumber;

  public ReplicaConnectHotResyncResponse() {
  }

  public ReplicaConnectHotResyncResponse(final long messageNumber) {
    this.messageNumber = messageNumber;
  }

  public long getMessageNumber() {
    return messageNumber;
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
