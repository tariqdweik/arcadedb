/*
 * Copyright (c) - Arcade Data LTD (https://arcadedata.com)
 */

package com.arcadedb.server.ha;

import com.arcadedb.database.Binary;

public class ReplicationMessage {
  public final long   messageNumber;
  public final Binary payload;

  public ReplicationMessage(final long messageNumber, final Binary payload) {
    this.messageNumber = messageNumber;
    this.payload = payload;
  }

  @Override
  public String toString() {
    return "number=" + messageNumber + " payload=" + payload.size() + " bytes";
  }
}