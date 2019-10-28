/*
 * Copyright (c) - Arcade Analytics LTD (https://arcadeanalytics.com)
 */
package com.arcadedb.server.ha.message;

import com.arcadedb.server.ha.HAServer;

/**
 * Response for a transaction. This is needed to check the quorum by the leader.
 */
public class TxResponse extends HAAbstractCommand {
  public TxResponse() {
  }

  @Override
  public HACommand execute(final HAServer server, final String remoteServerName, final long messageNumber) {
    server.receivedResponse(remoteServerName, messageNumber);
    return null;
  }

  @Override
  public String toString() {
    return "tx-response";
  }
}
