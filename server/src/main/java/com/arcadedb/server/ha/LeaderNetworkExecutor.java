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
import com.arcadedb.utility.FileUtils;
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
  private final    long                          joinedOn = System.currentTimeMillis();
  private          ChannelBinaryServer           channel;
  private volatile boolean                       shutdown = false;
  private final    String                        remoteServerName;
  private          Thread                        queueThread;

  // STATS
  private long totalMessages;
  private long totalBytes;
  private long latencyMin;
  private long latencyMax;
  private long latencyTotalTime;

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
        try {
          while (!shutdown || !queue.isEmpty()) {
            try {
              final Binary msg = queue.take();

              sendMessage(msg);

            } catch (IOException e) {
              server.getServer()
                  .log(this, Level.INFO, "Error on sending replication message to remote server '%s'", e, remoteServerName);
              shutdown = true;
              return;
            } catch (InterruptedException e) {
              // IGNORE IT
            }
          }
        } finally {
          queue.clear();
        }
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

        final HACommand response = request.execute(server, remoteServerName);
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
        server.getServer().log(this, Level.FINE, "Error on reading request from socket", e);
        close();
        server.removeServer(remoteServerName);
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
        queueThread = null;
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        // IGNORE IT
      }
    }

    if (channel != null)
      channel.close();
  }

  public void enqueueMessage(final Binary buffer) {
    if (shutdown)
      throw new ReplicationException("Error on replicating message because server is in shutdown mode");

    if (!queue.offer(buffer)) {
      // BACK-PRESSURE
      server.getServer().log(this, Level.WARNING, "Applying back-pressure on replicating messages to server '%s' (latency=%s)...",
          getRemoteServerName(), getLatencyStats());
      try {
        Thread.sleep(200);
      } catch (InterruptedException e) {
        // IGNORE IT
        Thread.currentThread().interrupt();
      }

      if (!queue.offer(buffer)) {
        try {
          Thread.sleep(600);
        } catch (InterruptedException e) {
          // IGNORE IT
          Thread.currentThread().interrupt();
        }

        if (!queue.offer(buffer)) {
          // QUEUE FULL, THE REMOTE SERVER COULD BE STUCK SOMEWHERE. REMOVE THE REPLICA
          queue.clear();
          close();
          throw new ReplicationException("Error on replicating to server '" + remoteServerName + "'");
        }
      }
    }
    totalBytes += buffer.size();
  }

  public String getRemoteServerName() {
    return remoteServerName;
  }

  public HACommand receiveResponse() throws IOException {
    final byte[] requestBytes = channel.readBytes();
    final byte requestId = requestBytes[0];
    return server.getMessageFactory().getCommand(requestId);
  }

  public long getJoinedOn() {
    return joinedOn;
  }

  public void updateStats(final long sentOn, final long receivedOn) {
    totalMessages++;

    final long delta = receivedOn - sentOn;
    latencyTotalTime += delta;

    if (latencyMin == -1 || delta < latencyMin)
      latencyMin = delta;
    if (delta > latencyMax)
      latencyMax = delta;
  }

  public String getLatencyStats() {
    if (totalMessages == 0)
      return "";
    return "avg=" + (latencyTotalTime / totalMessages) + " (min=" + latencyMin + " max=" + latencyMax + ")";
  }

  public String getThroughputStats() {
    if (totalBytes == 0)
      return "";
    return FileUtils.getSizeAsString(totalBytes) + " (" + FileUtils
        .getSizeAsString((int) (((double) totalBytes / (System.currentTimeMillis() - joinedOn)) * 1000)) + "/s)";
  }

  private void sendMessage(final Binary msg) throws IOException {
    channel.writeBytes(msg.getContent(), msg.size());
    channel.flush();
  }
}
