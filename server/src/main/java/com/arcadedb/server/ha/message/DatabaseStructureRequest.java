/*
 * Copyright (c) 2018 - Arcade Analytics LTD (https://arcadeanalytics.com)
 */
package com.arcadedb.server.ha.message;

import com.arcadedb.database.Binary;

public class DatabaseStructureRequest implements HARequestMessage {
  public final static byte   ID = 1;
  private             String databaseName;

  public DatabaseStructureRequest() {
  }

  public DatabaseStructureRequest(final String dbName) {
    this.databaseName = dbName;
  }

  @Override
  public void toStream(final Binary stream) {
    stream.putByte(ID);
    stream.putString(databaseName);
  }

  @Override
  public void fromStream(final Binary stream) {
    // SKIP THE COMMAND ID
    stream.getByte();

    databaseName = stream.getString();
  }

  public String getDatabaseName() {
    return databaseName;
  }

  @Override
  public byte getID() {
    return ID;
  }

  @Override
  public String toString() {
    return "dbstructure(" + databaseName + ")";
  }
}
