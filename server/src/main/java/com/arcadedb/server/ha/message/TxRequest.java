/*
 * Copyright (c) 2018 - Arcade Analytics LTD (https://arcadeanalytics.com)
 */
package com.arcadedb.server.ha.message;

import com.arcadedb.database.Binary;

/**
 * Replicate a transaction. No response is expected.
 */
public class TxRequest implements HARequestMessage {
  public final static byte ID = 3;

  private long   messageNumber;
  private String databaseName;
  private Binary bufferChanges;

  public TxRequest() {
  }

  public TxRequest(final long messageNumber, final String dbName, final Binary bufferChanges) {
    this.messageNumber = messageNumber;
    this.databaseName = dbName;
    this.bufferChanges = bufferChanges;
  }

  @Override
  public void toStream(final Binary stream) {
    stream.putByte(ID);

    stream.putLong(messageNumber);
    stream.putString(databaseName);
    stream.putBytes(bufferChanges.getContent(), bufferChanges.size());
  }

  @Override
  public void fromStream(final Binary stream) {
    // SKIP THE COMMAND ID
    stream.getByte();

    messageNumber = stream.getLong();
    databaseName = stream.getString();
    bufferChanges = new Binary(stream.getBytes());
  }

  public long getMessageNumber() {
    return messageNumber;
  }

  public String getDatabaseName() {
    return databaseName;
  }

  public Binary getBufferChange() {
    return bufferChanges;
  }

  @Override
  public byte getID() {
    return ID;
  }

  @Override
  public String toString() {
    return "tx(" + databaseName + ")";
  }

}
