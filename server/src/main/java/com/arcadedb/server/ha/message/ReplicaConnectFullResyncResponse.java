/*
 * Copyright (c) - Arcade Data LTD (https://arcadedata.com)
 */
package com.arcadedb.server.ha.message;

import com.arcadedb.database.Binary;
import com.arcadedb.server.ha.HAServer;

import java.util.HashSet;
import java.util.Set;

public class ReplicaConnectFullResyncResponse extends HAAbstractCommand {
  private long        lastMessageNumber;
  private Set<String> databases;

  public ReplicaConnectFullResyncResponse() {
  }

  public ReplicaConnectFullResyncResponse(final long lastMessageNumber, final Set<String> databases) {
    this.databases = databases;
  }

  @Override
  public HACommand execute(final HAServer server, final String remoteServerName, final long messageNumber) {
    return null;
  }

  @Override
  public void toStream(final Binary stream) {
    stream.putLong(lastMessageNumber);
    stream.putUnsignedNumber(databases.size());
    for (String db : databases)
      stream.putString(db);
  }

  @Override
  public void fromStream(final Binary stream) {
    lastMessageNumber = stream.getLong();
    databases = new HashSet<>();
    final int fileCount = (int) stream.getUnsignedNumber();
    for (int i = 0; i < fileCount; ++i)
      databases.add(stream.getString());
  }

  public long getLastMessageNumber() {
    return lastMessageNumber;
  }

  public Set<String> getDatabases() {
    return databases;
  }

  @Override
  public String toString() {
    return "fullResync(lastMessageNumber=" + lastMessageNumber + " dbs=" + databases + ")";
  }
}
