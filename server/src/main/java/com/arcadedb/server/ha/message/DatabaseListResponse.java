/*
 * Copyright (c) 2018 - Arcade Analytics LTD (https://arcadeanalytics.com)
 */
package com.arcadedb.server.ha.message;

import com.arcadedb.database.Binary;
import com.arcadedb.server.ha.HAServer;

import java.util.HashSet;
import java.util.Set;

public class DatabaseListResponse implements HACommand {
  private Set<String> databases;

  public DatabaseListResponse() {
  }

  public DatabaseListResponse(final Set<String> databases) {
    this.databases = databases;
  }

  @Override
  public HACommand execute(HAServer server) {
    return null;
  }

  @Override
  public void toStream(final Binary stream) {
    stream.putNumber(databases.size());
    for (String db : databases)
      stream.putString(db);
  }

  @Override
  public void fromStream(final Binary stream) {
    databases = new HashSet<>();
    final int fileCount = (int) stream.getNumber();
    for (int i = 0; i < fileCount; ++i)
      databases.add(stream.getString());
  }

  public Set<String> getDatabases() {
    return databases;
  }

  @Override
  public String toString() {
    return "dbs=" + databases;
  }
}
