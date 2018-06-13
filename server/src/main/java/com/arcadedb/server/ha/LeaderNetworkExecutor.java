/*
 * Copyright (c) 2018 - Arcade Analytics LTD (https://arcadeanalytics.com)
 */
package com.arcadedb.server.ha;

import com.arcadedb.Constants;
import com.arcadedb.GlobalConfiguration;
import com.arcadedb.database.Binary;
import com.arcadedb.network.binary.ChannelBinaryServer;
import com.arcadedb.network.binary.ConnectionException;
import com.arcadedb.server.ha.message.HACommand;
import com.conversantmedia.util.concurrent.PushPullBlockingQueue;

import java.io.EOFException;
import java.io.IOException;
import java.net.Socket;
import java.net.SocketException;
import java.util.logging.Level;

/**
 * This executor has an intermediate level of buffering managed with a queue. This avoids the Leader to be blocked in case the
 * remote replica does not read messages and the socket remains full causing a block in the sending of messages for all the
 * servers.
 */
public class LeaderNetworkExecutor extends Thread {
  public static final int PROTOCOL_VERSION = 0;

  private final    HAServer                      server;
  public final     PushPullBlockingQueue<Binary> queue    = new PushPullBlockingQueue<>(
      GlobalConfiguration.HA_REPLICATION_QUEUE_SIZE.getValueAsInteger());
  private          ChannelBinaryServer           channel;
  private volatile boolean                       shutdown = false;
  private final    String                        remoteServerName;
  private          Thread                        queueThread;

  public LeaderNetworkExecutor(final HAServer ha, final Socket socket) throws IOException {
    setName(Constants.PRODUCT + "-ha-leader2replica/" + socket.getInetAddress());

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

  @Override
  public void run() {
    queueThread = new Thread(new Runnable() {
      @Override
      public void run() {
        while (!shutdown) {
          try {
            final Binary msg = queue.take();

            sendMessage(msg);

          } catch (IOException e) {
            server.getServer()
                .log(this, Level.INFO, "Error on sending replication message to remote server '%s'", remoteServerName);
            shutdown = true;
          } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            break;
          }
        }

        queue.clear();
      }
    });
    queueThread.start();

    // REUSE THE SAME BUFFER TO AVOID MALLOC
    final Binary buffer = new Binary(1024);

    while (!shutdown) {
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

        final HACommand response = request.execute(server);
        if (response != null) {
          // SEND THE RESPONSE BACK (USING THE SAME BUFFER)
          buffer.reset();

          buffer.putByte(server.getMessageFactory().getCommandId(response));
          response.toStream(buffer);

          buffer.flip();

          server.getServer().log(this, Level.FINE, "Request %s -> %s", request, response);

          sendMessage(buffer);
        }

      } catch (EOFException | SocketException e) {
        server.getServer().log(this, Level.FINE, "Error on reading request", e);
        close();
      } catch (IOException e) {
        server.getServer().log(this, Level.SEVERE, "Error on reading request", e);
      }
    }
  }

  public void close() {
    shutdown = true;

    if (queueThread != null) {
      try {
        queueThread.interrupt();
        queueThread.join();
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        // IGNORE IT
      }
    }

    if (channel != null)
      channel.close();
  }

  public void enqueueMessage(final Binary buffer) {
    if (!queue.offer(buffer)) {
      // QUEUE FULL, THE REMOTE SERVER COULD BE STUCK SOMEWHERE. REMOVE THE REPLICA
      queue.clear();
      close();
      throw new ReplicationException("Error on replicating to server '" + remoteServerName + "'");
    }
  }

  public String getRemoteServerName() {
    return remoteServerName;
  }

  public HACommand receiveResponse() throws IOException {
    final byte[] requestBytes = channel.readBytes();
    final byte requestId = requestBytes[0];
    return server.getMessageFactory().getCommand(requestId);
  }

  private void sendMessage(final Binary msg) throws IOException {
    channel.writeBytes(msg.getContent(), msg.size());
    channel.flush();
  }
}
