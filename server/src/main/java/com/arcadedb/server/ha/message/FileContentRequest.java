/*
 * Copyright (c) 2018 - Arcade Analytics LTD (https://arcadeanalytics.com)
 */
package com.arcadedb.server.ha.message;

import com.arcadedb.database.Binary;

public class FileContentRequest implements HARequestMessage {
  public final static byte   ID = 2;
  private             String databaseName;
  private             int    fileId;
  private             int    from;

  public FileContentRequest() {
  }

  public FileContentRequest(final String dbName, final int fileId, final int from) {
    this.databaseName = dbName;
    this.fileId = fileId;
    this.from = from;
  }

  @Override
  public void toStream(final Binary stream) {
    stream.putByte(ID);

    stream.putString(databaseName);
    stream.putInt(fileId);
    stream.putInt(from);
  }

  @Override
  public void fromStream(final Binary stream) {
    // SKIP THE COMMAND ID
    stream.getByte();

    databaseName = stream.getString();
    fileId = stream.getInt();
    from = stream.getInt();
  }

  public String getDatabaseName() {
    return databaseName;
  }

  public int getFileId() {
    return fileId;
  }

  public int getFrom() {
    return from;
  }

  @Override
  public byte getID() {
    return ID;
  }

  @Override
  public String toString() {
    return "file(" + databaseName + "," + fileId + "," + from + ")";
  }
}
