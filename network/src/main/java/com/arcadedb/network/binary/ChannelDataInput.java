/*
 * Copyright (c) - Arcade Analytics LTD (https://arcadeanalytics.com)
 */

package com.arcadedb.network.binary;

import com.arcadedb.database.Database;
import com.arcadedb.database.RID;

import java.io.IOException;
import java.io.InputStream;

public interface ChannelDataInput {

  byte readByte() throws IOException;

  boolean readBoolean() throws IOException;

  int readInt() throws IOException;

  long readLong() throws IOException;

  short readShort() throws IOException;

  String readString() throws IOException;

  byte[] readBytes() throws IOException;

  RID readRID(Database database) throws IOException;

  int readVersion() throws IOException;

  InputStream getDataInput();
}
