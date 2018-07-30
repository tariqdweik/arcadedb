/*
 * Copyright (c) 2018 - Arcade Analytics LTD (https://arcadeanalytics.com)
 */
package com.arcadedb.server.ha.message;

import com.arcadedb.database.Binary;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.engine.CompressionFactory;
import com.arcadedb.engine.WALException;
import com.arcadedb.engine.WALFile;
import com.arcadedb.server.ha.HAServer;
import com.arcadedb.server.ha.ReplicationException;

import java.nio.channels.ClosedChannelException;
import java.util.logging.Level;

/**
 * Replicate a transaction. No response is expected.
 */
public class TxRequest extends HAAbstractCommand {
  private boolean waitForQuorum;
  private String  databaseName;
  private int     uncompressedLength;
  private Binary  bufferChanges;

  public TxRequest() {
  }

  public TxRequest(final String dbName, final Binary bufferChanges, final boolean waitForQuorum) {
    this.waitForQuorum = waitForQuorum;
    this.databaseName = dbName;

    bufferChanges.rewind();
    this.uncompressedLength = bufferChanges.size();
    this.bufferChanges = CompressionFactory.getDefault().compress(bufferChanges);
  }

  @Override
  public void toStream(final Binary stream) {
    stream.putByte((byte) (waitForQuorum ? 1 : 0));
    stream.putString(databaseName);
    stream.putInt(uncompressedLength);
    stream.putBytes(bufferChanges.getContent(), bufferChanges.size());
  }

  @Override
  public void fromStream(final Binary stream) {
    waitForQuorum = stream.getByte() == 1;
    databaseName = stream.getString();
    uncompressedLength = stream.getInt();
    bufferChanges = CompressionFactory.getDefault().decompress(new Binary(stream.getBytes()), uncompressedLength);
  }

  @Override
  public HACommand execute(final HAServer server, final String remoteServerName, final long messageNumber) {
    final WALFile.WALTransaction tx = getTx();

    final DatabaseInternal db = (DatabaseInternal) server.getServer().getDatabase(databaseName);
    if (!db.isOpen())
      throw new ReplicationException("Database '" + databaseName + "' is closed");

    try {
      db.getTransactionManager().applyChanges(tx);

    } catch (WALException e) {
      if (e.getCause() instanceof ClosedChannelException) {
        // CLOSE THE ENTIRE DB
        server.getServer().log(this, Level.SEVERE, "Closed file during transaction, closing the entire database (error=%s)", e.toString());
        db.getEmbedded().close();
      }
      throw e;
    } finally {
      db.close();
    }

    if (waitForQuorum)
      return new TxResponse();

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
