/*
 * Copyright (c) 2018 - Arcade Analytics LTD (https://arcadeanalytics.com)
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
}