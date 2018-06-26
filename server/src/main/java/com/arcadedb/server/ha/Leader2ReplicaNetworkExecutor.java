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
public class Leader2ReplicaNetworkExecutor extends Thread {
  public static final int PROTOCOL_VERSION = 0;

  private final    HAServer                      server;
  public final     PushPullBlockingQueue<Binary> queue    = new PushPullBlockingQueue<>(
      GlobalConfiguration.HA_REPLICATION_QUEUE_SIZE.getValueAsInteger());
  private final    long                          joinedOn = System.currentTimeMillis();
  private          long                          leftOn   = 0;
  private          ChannelBinaryServer           channel;
  private volatile boolean                       shutdown = false;
  private final    String                        remoteServerName;
  private          Thread                        queueThread;
  private          boolean                       online   = false;

  // STATS
  private long totalMessages;
  private long totalBytes;
  private long latencyMin;
  private long latencyMax;
  private long latencyTotalTime;

  public Leader2ReplicaNetworkExecutor(final HAServer ha, final Socket socket) throws IOException {
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

  public void mergeFrom(final Leader2ReplicaNetworkExecutor previousConnection) {
    queue.addAll(previousConnection.queue);
  }

  @Override
  public void run() {
    queueThread = new Thread(new Runnable() {
      @Override
      public void run() {
        try {
          while (!shutdown || !queue.isEmpty()) {
            if (!online) {
              server.getServer()
                  .log(this, Level.INFO, "Waiting on sending message to replica '%s' because still OFFLINE (buffered=%d)",
                      remoteServerName, queue.size());
              try {
                Thread.sleep(200);
                continue;
              } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
              }
            }

            try {
              final Binary msg = queue.take();

              server.getServer()
                  .log(this, Level.FINE, "Sending message to replica '%s' (buffered=%d)...", remoteServerName, queue.size());

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

          server.getServer()
              .log(this, Level.FINE, "Replication thread to remote server '%s' is off (buffered=%d)", remoteServerName,
                  queue.size());

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
        server.setReplicaStatus(remoteServerName, false);
      } catch (IOException e) {
        server.getServer().log(this, Level.SEVERE, "Error on reading request", e);
      }
    }
  }

  public void close() {
    shutdown = true;

    try {
      if (queueThread != null) {
        try {
          queueThread.interrupt();
          queueThread.join(5000);
          queueThread = null;
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          // IGNORE IT
        }
      }

      if (channel != null)
        channel.close();
    } catch (Exception e) {
      // IGNORE IT
    }
  }

  public void enqueueMessage(final Binary buffer) {
    if (shutdown)
      throw new ReplicationException("Error on replicating message because server is in shutdown mode");

    if (queue.size() > 1)
      server.getServer().log(this, Level.FINE, "Buffering request to server '%s' (status=%s buffered=%d)", remoteServerName,
          online ? "ONLINE" : "OFFLINE", queue.size());

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
          server.setReplicaStatus(remoteServerName, false);
          throw new ReplicationException("Error on replicating to server '" + remoteServerName + "'");
        }
      }
    }
    totalBytes += buffer.size();
  }

  public void setStatus(final boolean online) {
    if (this.online == online)
      // NO STATUS CHANGE
      return;

    this.online = online;
    this.server.getServer().log(this, Level.INFO, "Replica server '%s' is %s", remoteServerName, online ? "ONLINE" : "OFFLINE");
    this.leftOn = online ? 0 : System.currentTimeMillis();

    if (!online)
      close();
  }

  public String getRemoteServerName() {
    return remoteServerName;
  }

  public long getJoinedOn() {
    return joinedOn;
  }

  public long getLeftOn() {
    return leftOn;
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

  public boolean isOnline() {
    return online;
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
