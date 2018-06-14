/*
 * Copyright (c) 2018 - Arcade Analytics LTD (https://arcadeanalytics.com)
 */
package com.arcadedb.server.ha;

import com.arcadedb.Constants;
import com.arcadedb.GlobalConfiguration;
import com.arcadedb.database.Binary;
import com.arcadedb.network.binary.ChannelBinaryClient;
import com.arcadedb.server.TestCallback;
import com.arcadedb.server.ha.message.HACommand;

import java.io.EOFException;
import java.io.IOException;
import java.net.SocketException;
import java.net.SocketTimeoutException;
import java.util.logging.Level;

public class ReplicaNetworkExecutor extends Thread {
  private final    HAServer            server;
  private          ChannelBinaryClient channel;
  private volatile boolean             shutdown = false;
  private final    boolean             testOn;

  public ReplicaNetworkExecutor(final HAServer ha, final ChannelBinaryClient channel) {
    setName(Constants.PRODUCT + "-ha-replica2leader/" + channel.getURL());
    this.server = ha;
    this.channel = channel;
    this.testOn = GlobalConfiguration.TEST.getValueAsBoolean();
  }

  @Override
  public void run() {
    // REUSE THE SAME BUFFER TO AVOID MALLOC
    final Binary buffer = new Binary(1024);

    while (!shutdown || channel.inputHasData()) {
      try {
        final byte[] requestBytes = channel.readBytes();

        final byte requestId = requestBytes[0];

        final HACommand request = server.getMessageFactory().getCommand(requestId);

        if (request == null) {
          server.getServer().log(this, Level.INFO, "Error on reading request, command %d not valid", requestId);
          channel.clearInput();
          continue;
        }

        buffer.reset();
        buffer.putByteArray(requestBytes);
        buffer.flip();

        // SKIP COMMAND ID
        buffer.getByte();

        request.fromStream(buffer);

        server.getServer().log(this, Level.FINE, "Received request from the leader '%s'", request);

        final HACommand response = request.execute(server, null);

        if (testOn)
          server.getServer().lifecycleEvent(TestCallback.TYPE.REPLICA_MSG_RECEIVED, request);

        if (response != null)
          sendCommandToLeader(buffer, response);

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

  public void sendCommandToLeader(final Binary buffer, final HACommand response) throws IOException {
    server.getServer().log(this, Level.FINE, "Sending response back to the leader '%s'...", response);

    buffer.reset();

    buffer.putByte(server.getMessageFactory().getCommandId(response));
    response.toStream(buffer);

    buffer.flip();

    channel.writeBytes(buffer.getContent(), buffer.size());
    channel.flush();
  }

  public void close() {
    shutdown = true;
    if (channel != null)
      channel.close();
  }

  public String getURL() {
    return channel.getURL();
  }

  public byte[] receiveResponse() throws IOException {
    return channel.readBytes();
  }
}
