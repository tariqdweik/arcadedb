/*
 * Copyright (c) 2018 - Arcade Analytics LTD (https://arcadeanalytics.com)
 */
package com.arcadedb.server.ha.message;

import com.arcadedb.server.ha.HAServer;

/**
 * Response for a transaction. This is needed to check the quorum by the leader.
 */
public class TxResponse extends HAAbstractCommand {
  @Override
  public HACommand execute(final HAServer server, final String remoteServerName, final long messageNumber) {
    server.receivedResponseForQuorum(remoteServerName, messageNumber);
    return null;
  }

  @Override
  public String toString() {
    return "tx-response";
  }
}
