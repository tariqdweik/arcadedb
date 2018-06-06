/*
 * Copyright (c) 2018 - Arcade Analytics LTD (https://arcadeanalytics.com)
 */
package com.arcadedb.server.ha;

import com.arcadedb.Constants;
import com.arcadedb.database.Binary;
import com.arcadedb.network.binary.ChannelBinaryServer;
import com.arcadedb.network.binary.ConnectionException;
import com.arcadedb.network.binary.NetworkProtocolException;
import com.arcadedb.server.ha.message.HARequestMessage;
import com.arcadedb.server.ha.message.HAResponseMessage;

import java.io.EOFException;
import java.io.IOException;
import java.net.Socket;
import java.net.SocketException;
import java.util.logging.Level;

public class LeaderNetworkExecutor extends Thread {
  public static final int PROTOCOL_VERSION = 0;

  private final    HAServer            server;
  private          ChannelBinaryServer channel;
  private volatile boolean             shutdown = false;
  private final String remoteServerName;

  public LeaderNetworkExecutor(final HAServer ha, final Socket socket) throws IOException {
    setName(Constants.PRODUCT + "-ha-leader/" + socket.getInetAddress());

    this.server = ha;
    this.channel = new ChannelBinaryServer(socket);

    try {
      if (!ha.isLeader()) {
        this.channel.writeBoolean(false);
        this.channel.writeString("Current server '" + ha.getServerName() + "' is not the leader");
        throw new ConnectionException(socket.getInetAddress().toString(),
            "Current server '" + ha.getServerName() + "' is not the leader");
      }

      final short remoteProtocolVersion = this.channel.readShort();
      if (remoteProtocolVersion != PROTOCOL_VERSION) {
        this.channel.writeBoolean(false);
        this.channel.writeString(
            "Network protocol version " + remoteProtocolVersion + " is different than local server " + PROTOCOL_VERSION);
        throw new ConnectionException(socket.getInetAddress().toString(),
            "Network protocol version " + remoteProtocolVersion + " is different than local server " + PROTOCOL_VERSION);
      }

      final String remoteClusterName = this.channel.readString();
      if (!remoteClusterName.equals(ha.getClusterName())) {
        this.channel.writeBoolean(false);
        this.channel.writeString("Cluster name '" + remoteClusterName + "' does not match");
        throw new ConnectionException(socket.getInetAddress().toString(),
            "Cluster name '" + remoteClusterName + "' does not match");
      }

      remoteServerName = this.channel.readString();

      ha.getServer().log(this, Level.INFO, "Remote server '%s' successfully connected", remoteServerName);

      // CONNECTED
      this.channel.writeBoolean(true);

    } finally {
      this.channel.flush();
    }
  }

  public String getRemoteServerName() {
    return remoteServerName;
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

        final HAResponseMessage response = server.getMessageFactory().getResponseMessage(request.getID());
        if (response == null)
          throw new NetworkProtocolException("Response message for request id " + request.getID() + " not found");

        response.build(server, request);

        server.getServer().log(this, Level.INFO, "Request %s -> %s", request, response);

        // SEND THE RESPONSE BACK (USING THE SAME BUFFER)
        buffer.reset();
        response.toStream(buffer);
        buffer.flip();

        sendRequest(buffer);

      } catch (EOFException | SocketException e) {
        server.getServer().log(this, Level.FINE, "Error on reading request", e);
        close();
      } catch (IOException e) {
        server.getServer().log(this, Level.SEVERE, "Error on reading request", e);
      }
    }

  }

  public void sendRequest(final Binary buffer) throws IOException {
    channel.writeBytes(buffer.getContent(), buffer.size());
    channel.flush();
  }

  public void close() {
    shutdown = true;
    if (channel != null)
      channel.close();
  }
}
