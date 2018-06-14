/*
 * Copyright (c) 2018 - Arcade Analytics LTD (https://arcadeanalytics.com)
 */
package com.arcadedb.server.ha.message;

import com.arcadedb.database.Binary;
import com.arcadedb.server.ha.HAServer;

/**
 * Response for a transaction. This is needed to check the quorum by the leader.
 */
public class TxResponse implements HACommand {
  private long messageNumber;

  public TxResponse() {
  }

  public TxResponse(final long messageNumber) {
    this.messageNumber = messageNumber;
  }

  @Override
  public void toStream(final Binary stream) {
    stream.putLong(messageNumber);
  }

  @Override
  public void fromStream(final Binary stream) {
    messageNumber = stream.getLong();
  }

  @Override
  public HACommand execute(final HAServer server, final String remoteServerName) {
    server.receivedResponseForQuorum(remoteServerName, messageNumber);
    return null;
  }

  @Override
  public String toString() {
    return "tx-response(" + messageNumber + ")";
  }
}
