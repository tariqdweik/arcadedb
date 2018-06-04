/*
 * Copyright (c) 2018 - Arcade Analytics LTD (https://arcadeanalytics.com)
 */
package com.arcadedb.server.ha.message;

import com.arcadedb.database.Binary;
import com.arcadedb.server.ha.HAServer;

import java.util.HashSet;
import java.util.Set;

public class DatabaseListResponse implements HAResponseMessage<DatabaseListRequest> {
  public final static byte        ID = DatabaseListRequest.ID;
  private             Set<String> databases;

  public DatabaseListResponse() {
  }

  public Set<String> getDatabases() {
    return databases;
  }

  @Override
  public void toStream(final Binary stream) {
    stream.putByte(ID);
    stream.putNumber(databases.size());
    for (String db : databases)
      stream.putString(db);
  }

  @Override
  public void fromStream(final Binary stream) {
    // SKIP THE COMMAND ID
    stream.getByte();

    databases = new HashSet<>();
    final int fileCount = (int) stream.getNumber();
    for (int i = 0; i < fileCount; ++i)
      databases.add(stream.getString());
  }

  @Override
  public void build(final HAServer server, final DatabaseListRequest request) {
    databases = server.getServer().getDatabases();
  }

  @Override
  public String toString() {
    return "dbs=" + databases;
  }
}
