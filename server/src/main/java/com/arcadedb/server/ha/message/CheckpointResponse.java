/*
 * Copyright (c) 2018 - Arcade Analytics LTD (https://arcadeanalytics.com)
 */
package com.arcadedb.server.ha.message;

import com.arcadedb.database.Binary;
import com.arcadedb.server.ha.HAServer;

import java.util.logging.Level;

public class CheckpointResponse implements HACommand {
  private String replicaName;
  private String databaseName;
  private long   messageNumber;
  private long   txId;

  public CheckpointResponse() {
  }

  public CheckpointResponse(final String replicaName, final String databaseName, final Long[] last) {
    this.replicaName = replicaName;
    this.databaseName = databaseName;

    if (last != null) {
      messageNumber = last[0];
      txId = last[1];
    } else {
      // NO MESSAGES YET
      messageNumber = -1;
      txId = -1;
    }
  }

  @Override
  public HACommand execute(final HAServer server) {
    server.updateReplicaCheckpoint(replicaName, databaseName, new Long[] { messageNumber, txId, System.currentTimeMillis() });

    server.getServer()
        .log(this, Level.FINE, "Received checkpoint server=%s db=%s messageNumber=%d txId=%d", replicaName, databaseName,
            messageNumber, txId);

    return null;
  }

  @Override
  public void toStream(final Binary stream) {
    stream.putString(replicaName);
    stream.putString(databaseName);
    stream.putLong(messageNumber);
    stream.putLong(txId);
  }

  @Override
  public void fromStream(final Binary stream) {
    replicaName = stream.getString();
    databaseName = stream.getString();
    messageNumber = stream.getLong();
    txId = stream.getLong();
  }

  @Override
  public String toString() {
    return "replicaName=" + replicaName + "databaseName= " + databaseName + " messageNumber=" + messageNumber + " txId=" + txId;
  }
}
