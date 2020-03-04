/*
 * Copyright (c) - Arcade Data LTD (https://arcadedata.com)
 */
package com.arcadedb.server.ha.message;

import com.arcadedb.server.ha.HAServer;

/**
 * Response for forwarded transaction.
 */
public class TxForwardResponse extends HAAbstractCommand {
  public TxForwardResponse() {
  }

  @Override
  public HACommand execute(final HAServer server, final String remoteServerName, final long messageNumber) {
    server.receivedResponseFromForward(messageNumber, null);
    return null;
  }

  @Override
  public String toString() {
    return "forward-response";
  }
}
