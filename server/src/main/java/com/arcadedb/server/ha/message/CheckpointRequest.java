/*
 * Copyright (c) 2018 - Arcade Analytics LTD (https://arcadeanalytics.com)
 */
package com.arcadedb.server.ha.message;

import com.arcadedb.database.Binary;
import com.arcadedb.server.ha.HAServer;

/**
 * Ask last MessageID & TxId committed.
 */
public class CheckpointRequest implements HACommand {
  private String databaseName;

  public CheckpointRequest() {
  }

  public CheckpointRequest(final String dbName) {
    this.databaseName = dbName;
  }

  @Override
  public HACommand execute(final HAServer server) {
    return new CheckpointResponse(server.getServerName(), databaseName, server.getLastMessage(databaseName));
  }

  @Override
  public void toStream(final Binary stream) {
    stream.putString(databaseName);
  }

  @Override
  public void fromStream(final Binary stream) {
    databaseName = stream.getString();
  }

  @Override
  public String toString() {
    return "checkpoint(" + databaseName + ")";
  }

}
