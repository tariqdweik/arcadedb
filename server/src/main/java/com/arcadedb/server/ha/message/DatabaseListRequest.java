/*
 * Copyright (c) 2018 - Arcade Analytics LTD (https://arcadeanalytics.com)
 */
package com.arcadedb.server.ha.message;

import com.arcadedb.database.Binary;

public class DatabaseListRequest implements HARequestMessage {
  public final static byte ID = 0;

  public DatabaseListRequest() {
  }

  @Override
  public void toStream(final Binary stream) {
    stream.putByte(ID);
  }

  @Override
  public void fromStream(final Binary stream) {
    // SKIP THE COMMAND ID
    stream.getByte();
  }

  @Override
  public byte getID() {
    return ID;
  }

  @Override
  public String toString() {
    return "dbs()";
  }
}
