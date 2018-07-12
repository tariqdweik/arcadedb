/*
 * Copyright (c) 2018 - Arcade Analytics LTD (https://arcadeanalytics.com)
 */
package com.arcadedb.server.ha.message;

import com.arcadedb.database.Binary;
import com.arcadedb.server.ha.HAServer;

import java.util.logging.Level;

public class ReplicaConnectRequest extends HAAbstractCommand {
  private long lastReplicationMessageNumber = -1;

  public ReplicaConnectRequest() {
  }

  public ReplicaConnectRequest(final long lastReplicationMessageNumber) {
    this.lastReplicationMessageNumber = lastReplicationMessageNumber;
  }

  @Override
  public HACommand execute(final HAServer server, final String remoteServerName, final long messageNumber) {
    if (lastReplicationMessageNumber > -1) {
      final long lastPosition = server.getReplicationLogFile().findMessagePosition(lastReplicationMessageNumber);
      if (lastPosition > -1) {
        server.getServer()
            .log(this, Level.INFO, "Hot backup with Replica server '%s' is possible (lastReplicationMessageNumber=%d lastPosition=%d)", remoteServerName,
                lastReplicationMessageNumber, lastPosition);
        return new ReplicaConnectHotResyncResponse(lastPosition);
      }
    }

    // IN ANY OTHER CASE EXECUTE FULL SYNC
    return new ReplicaConnectFullResyncResponse(server.getReplicationLogFile().getLastMessageNumber(), server.getServer().getDatabaseNames());
  }

  @Override
  public void toStream(Binary stream) {
    stream.putLong(lastReplicationMessageNumber);
  }

  @Override
  public void fromStream(Binary stream) {
    lastReplicationMessageNumber = stream.getLong();
  }

  @Override
  public String toString() {
    return "connect(" + lastReplicationMessageNumber + ")";
  }
}
