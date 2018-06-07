/*
 * Copyright (c) 2018 - Arcade Analytics LTD (https://arcadeanalytics.com)
 */
package com.arcadedb.server.ha.message;

import com.arcadedb.database.Binary;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.engine.WALFile;
import com.arcadedb.server.ha.HAServer;
import com.arcadedb.server.ha.ReplicationException;

/**
 * Replicate a transaction. No response is expected.
 */
public class TxRequest implements HACommand {
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
    stream.putLong(messageNumber);
    stream.putString(databaseName);
    stream.putBytes(bufferChanges.getContent(), bufferChanges.size());
  }

  @Override
  public void fromStream(final Binary stream) {
    messageNumber = stream.getLong();
    databaseName = stream.getString();
    bufferChanges = new Binary(stream.getBytes());
  }

  @Override
  public HACommand execute(final HAServer server) {
    final WALFile.WALTransaction tx = getTx();

    final DatabaseInternal db = (DatabaseInternal) server.getServer().getDatabase(databaseName);
    try {
      db.getTransactionManager().applyChanges(tx);

      // UPDATE LAST COMMITTED MESSAGE NUMBER + TXID. CHECKPOINT WILL REGULARLY CHECK THE STATUS OF REPLICATION ASKING FOR THIS PAIR OF NUMBERS
      server.updateLastMessage(databaseName, new Long[] { messageNumber, tx.txId, System.currentTimeMillis() });

    } finally {
      db.close();
    }

    return null;
  }

  @Override
  public String toString() {
    return "tx(" + databaseName + ")";
  }

  private WALFile.WALTransaction getTx() {

    final WALFile.WALTransaction tx = new WALFile.WALTransaction();

    final Binary bufferChange = bufferChanges;

    int pos = 0;
    tx.txId = bufferChange.getLong(pos);
    pos += Binary.LONG_SERIALIZED_SIZE;

    final int pages = bufferChange.getInt(pos);
    pos += Binary.INT_SERIALIZED_SIZE;

    final int segmentSize = bufferChange.getInt(pos);
    pos += Binary.INT_SERIALIZED_SIZE;

    if (pos + segmentSize + Binary.LONG_SERIALIZED_SIZE > bufferChange.size())
      // TRUNCATED FILE
      throw new ReplicationException("Replicated transaction buffer is corrupted");

    tx.pages = new WALFile.WALPage[pages];

    for (int i = 0; i < pages; ++i) {
      if (pos > bufferChange.size())
        // INVALID
        throw new ReplicationException("Replicated transaction buffer is corrupted");

      tx.pages[i] = new WALFile.WALPage();

      tx.pages[i].fileId = bufferChange.getInt(pos);
      pos += Binary.INT_SERIALIZED_SIZE;

      tx.pages[i].pageNumber = bufferChange.getInt(pos);
      pos += Binary.INT_SERIALIZED_SIZE;

      tx.pages[i].changesFrom = bufferChange.getInt(pos);
      pos += Binary.INT_SERIALIZED_SIZE;

      tx.pages[i].changesTo = bufferChange.getInt(pos);
      pos += Binary.INT_SERIALIZED_SIZE;

      final int deltaSize = tx.pages[i].changesTo - tx.pages[i].changesFrom + 1;

      tx.pages[i].currentPageVersion = bufferChange.getInt(pos);
      pos += Binary.INT_SERIALIZED_SIZE;

      tx.pages[i].currentPageSize = bufferChange.getInt(pos);
      pos += Binary.INT_SERIALIZED_SIZE;

      final byte[] buffer = new byte[deltaSize];

      tx.pages[i].currentContent = new Binary(buffer);
      bufferChange.getByteArray(pos, buffer, 0, deltaSize);

      pos += deltaSize;
    }

    final long mn = bufferChange.getLong(pos + Binary.INT_SERIALIZED_SIZE);
    if (mn != WALFile.MAGIC_NUMBER)
      // INVALID
      throw new ReplicationException("Replicated transaction buffer is corrupted");

    return tx;
  }

}
