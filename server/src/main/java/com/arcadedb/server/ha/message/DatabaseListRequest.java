/*
 * Copyright (c) 2018 - Arcade Analytics LTD (https://arcadeanalytics.com)
 */
package com.arcadedb.server.ha.message;

import com.arcadedb.database.Binary;
import com.arcadedb.server.ha.HAServer;

public class DatabaseListRequest implements HACommand {
  public DatabaseListRequest() {
  }

  @Override
  public HACommand execute(final HAServer server, String remoteServerName) {
    return new DatabaseListResponse(server.getServer().getDatabaseNames());
  }

  @Override
  public void toStream(final Binary stream) {
  }

  @Override
  public void fromStream(final Binary stream) {
  }

  @Override
  public String toString() {
    return "dbs()";
  }
}
