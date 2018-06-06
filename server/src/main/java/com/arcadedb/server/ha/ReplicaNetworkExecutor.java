/*
 * Copyright (c) 2018 - Arcade Analytics LTD (https://arcadeanalytics.com)
 */
package com.arcadedb.server.ha;

import com.arcadedb.Constants;
import com.arcadedb.database.Binary;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.engine.WALFile;
import com.arcadedb.network.binary.ChannelBinaryClient;
import com.arcadedb.server.ha.message.HARequestMessage;
import com.arcadedb.server.ha.message.TxRequest;

import java.io.EOFException;
import java.io.IOException;
import java.net.SocketException;
import java.net.SocketTimeoutException;
import java.util.logging.Level;

public class ReplicaNetworkExecutor extends Thread {
  private final    HAServer            server;
  private          ChannelBinaryClient channel;
  private volatile boolean             shutdown = false;

  public ReplicaNetworkExecutor(final HAServer ha, final ChannelBinaryClient channel) {
    setName(Constants.PRODUCT + "-ha-replica/" + channel.getURL());
    this.server = ha;
    this.channel = channel;
  }

  @Override
  public void run() {
    // REUSE THE SAME BUFFER TO AVOID MALLOC
    final Binary buffer = new Binary(1024);

    while (!shutdown) {
      try {
        final byte[] requestBytes = channel.readBytes();

        final byte requestId = requestBytes[0];

        final HARequestMessage request = server.getMessageFactory().getRequestMessage(requestId);

        if (request == null) {
          server.getServer().log(this, Level.INFO, "Error on reading request, command %d not valid", requestId);
          channel.clearInput();
          continue;
        }

        buffer.reset();
        buffer.putByteArray(requestBytes);
        buffer.flip();

        request.fromStream(buffer);

        if (request instanceof TxRequest) {
          server.getServer().log(this, Level.FINE, "Applying transaction (msgNum=%d)", ((TxRequest) request).getMessageNumber());
          applyTx((TxRequest) request);
        } else
          server.getServer().log(this, Level.SEVERE, "Unknown request received from the leader %s", request);

      } catch (EOFException | SocketException e) {
        server.getServer().log(this, Level.FINE, "Error on reading request", e);
        close();
      } catch (SocketTimeoutException e) {
        // IGNORE IT
      } catch (IOException e) {
        server.getServer().log(this, Level.SEVERE, "Error on reading request", e);
      }
    }

  }

  private void applyTx(final TxRequest request) {
    final WALFile.WALTransaction tx = getTx(request);

    final DatabaseInternal db = (DatabaseInternal) server.getServer().getDatabase(request.getDatabaseName());
    try {
      db.getTransactionManager().applyChanges(tx);
    } finally {
      db.close();
    }
  }

  private WALFile.WALTransaction getTx(final TxRequest request) {

    final WALFile.WALTransaction tx = new WALFile.WALTransaction();

    final Binary bufferChange = request.getBufferChange();

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

  public void close() {
    shutdown = true;
    if (channel != null)
      channel.close();
  }

  public String getURL() {
    return channel.getURL();
  }

  public void sendRequest(final Binary buffer) throws IOException {
    channel.writeBytes(buffer.getContent(), buffer.size());
    channel.flush();
  }

  public byte[] receiveResponse() throws IOException {
    return channel.readBytes();
  }
}
