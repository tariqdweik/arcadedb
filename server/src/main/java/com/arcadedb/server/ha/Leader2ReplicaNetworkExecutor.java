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
import com.arcadedb.server.ha.message.ReplicaConnectHotResyncResponse;
import com.arcadedb.utility.Callable;
import com.arcadedb.utility.FileUtils;
import com.arcadedb.utility.LogManager;
import com.arcadedb.utility.Pair;
import com.conversantmedia.util.concurrent.PushPullBlockingQueue;

import java.io.EOFException;
import java.io.IOException;
import java.net.SocketException;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;

/**
 * This executor has an intermediate level of buffering managed with a queue. This avoids the Leader to be blocked in case the
 * remote replica does not read messages and the socket remains full causing a block in the sending of messages for all the
 * servers.
 */
public class Leader2ReplicaNetworkExecutor extends Thread {
  public enum STATUS {
    JOINING, OFFLINE, ONLINE
  }

  private final    HAServer                      server;
  private final    String                        remoteServerName;
  private final    String                        remoteServerAddress;
  private final    String                        remoteServerHTTPAddress;
  private final    PushPullBlockingQueue<Binary> queue                 = new PushPullBlockingQueue<>(
      GlobalConfiguration.HA_REPLICATION_QUEUE_SIZE.getValueAsInteger());
  private          long                          joinedOn;
  private          long                          leftOn                = 0;
  private          ChannelBinaryServer           channel;
  private          Thread                        queueThread;
  private          STATUS                        status                = STATUS.JOINING;
  private          Object                        lock                  = new Object();
  private          Object                        channelOutputLock     = new Object();
  private          Object                        channelInputLock      = new Object();
  private volatile boolean                       shutdownCommunication = false;

  // STATS
  private long totalMessages;
  private long totalBytes;
  private long latencyMin;
  private long latencyMax;
  private long latencyTotalTime;

  public Leader2ReplicaNetworkExecutor(final HAServer ha, final ChannelBinaryServer channel, final String remoteServerName, final String remoteServerAddress,
      final String remoteServerHTTPAddress) throws IOException {
    this.server = ha;
    this.remoteServerName = remoteServerName;
    this.remoteServerAddress = remoteServerAddress;
    this.remoteServerHTTPAddress = remoteServerHTTPAddress;
    this.channel = channel;

    setName(Constants.PRODUCT + "-ha-leader2replica/" + server.getServer().getServerName() + "/?");

    synchronized (channelOutputLock) {
      try {
        if (!ha.isLeader()) {
          this.channel.writeBoolean(false);
          this.channel.writeByte(ReplicationProtocol.ERROR_CONNECT_NOLEADER);
          this.channel.writeString("Current server '" + ha.getServerName() + "' is not the Leader");
          this.channel.writeString(server.getLeader().getRemoteServerName());
          this.channel.writeString(server.getLeader().getRemoteAddress());
          throw new ConnectionException(channel.socket.getInetAddress().toString(), "Current server '" + ha.getServerName() + "' is not the Leader");
        }

        final HAServer.ELECTION_STATUS electionStatus = ha.getElectionStatus();
        if (electionStatus != HAServer.ELECTION_STATUS.DONE && electionStatus != HAServer.ELECTION_STATUS.LEADER_WAITING_FOR_QUORUM) {
          this.channel.writeBoolean(false);
          this.channel.writeByte(ReplicationProtocol.ERROR_CONNECT_ELECTION_PENDING);
          this.channel.writeString("Election for the Leader is pending");
          throw new ConnectionException(channel.socket.getInetAddress().toString(), "Election for Leader is pending");
        }

        setName(Constants.PRODUCT + "-ha-leader2replica/" + server.getServer().getServerName() + "/" + remoteServerName + "(" + remoteServerAddress + ")");

        // CONNECTED
        this.channel.writeBoolean(true);

        this.channel.writeString(server.getServerName());
        this.channel.writeLong(server.lastElectionVote != null ? server.lastElectionVote.getFirst() : 1);
        this.channel.writeString(server.getServer().getHttpServer().getListeningAddress());
        this.channel.writeString(this.server.getServerAddressList());

        ha.getServer().log(this, Level.INFO, "Remote Replica server '%s' (%s) successfully connected", remoteServerName, remoteServerAddress);

      } finally {
        this.channel.flush();
      }
    }
  }

  public void mergeFrom(final Leader2ReplicaNetworkExecutor previousConnection) {
    lock = previousConnection.lock;
    queue.addAll(previousConnection.queue);
    previousConnection.close();
  }

  @Override
  public void run() {
    LogManager.instance().setContext(server.getServerName());
    queueThread = new Thread(new Runnable() {
      @Override
      public void run() {
        LogManager.instance().setContext(server.getServerName());
        Binary lastMessage = null;
        while (!shutdownCommunication || !queue.isEmpty()) {
          try {
            if (lastMessage == null)
              lastMessage = queue.poll(500, TimeUnit.MILLISECONDS);

            if (lastMessage == null)
              continue;

            if (shutdownCommunication)
              break;

            switch (status) {
            case ONLINE:

              server.getServer().log(this, Level.FINE, "Sending message to replica '%s' (buffered=%d)...", remoteServerName, queue.size());

              sendMessage(lastMessage);
              lastMessage = null;
              break;

            default:
              Thread.sleep(500);
              continue;
            }

          } catch (IOException e) {
            server.getServer().log(this, Level.INFO, "Error on sending replication message to remote server '%s' (error=%s)", remoteServerName, e);
            shutdownCommunication = true;
            return;
          } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            break;
          }
        }

        server.getServer().log(this, Level.FINE, "Replication thread to remote server '%s' is off (buffered=%d)", remoteServerName, queue.size());

      }
    });
    queueThread.start();
    queueThread.setName(Constants.PRODUCT + "-ha-leader2replica-sender/" + server.getServer().getServerName() + "/" + remoteServerName);

    // REUSE THE SAME BUFFER TO AVOID MALLOC
    final Binary buffer = new Binary(1024);

    while (!shutdownCommunication) {
      try {
        final Pair<ReplicationMessage, HACommand> request = server.getMessageFactory().deserializeCommand(buffer, readRequest());

        if (request == null) {
          channel.clearInput();
          continue;
        }

        final ReplicationMessage message = request.getFirst();

        final HACommand response = request.getSecond().execute(server, remoteServerName, message.messageNumber);
        if (response != null) {
          // SEND THE RESPONSE BACK (USING THE SAME BUFFER)
          server.getMessageFactory().serializeCommand(response, buffer, message.messageNumber);

          server.getServer().log(this, Level.FINE, "Request %s -> %s to '%s'", request.getSecond(), response, remoteServerName);

          sendMessage(buffer);

          if (response instanceof ReplicaConnectHotResyncResponse) {
            server.resendMessagesToReplica(((ReplicaConnectHotResyncResponse) response).getPositionInLog(), remoteServerName);
            server.setReplicaStatus(remoteServerName, true);
          }
        }

      } catch (EOFException | SocketException e) {
        server.getServer().log(this, Level.FINE, "Error on reading request from socket", e);
        server.setReplicaStatus(remoteServerName, false);
        close();
      } catch (IOException e) {
        server.getServer().log(this, Level.SEVERE, "Error on reading request", e);
        close();
      }
    }
  }

  private byte[] readRequest() throws IOException {
    synchronized (channelInputLock) {
      return channel.readBytes();
    }
  }

  /**
   * Test purpose only.
   */
  public void closeChannel() {
    final ChannelBinaryServer c = channel;
    if (c != null) {
      c.close();
      channel = null;
    }
  }

  public void close() {
    shutdownCommunication = true;

    try {
      final Thread qt = queueThread;
      if (qt != null) {
        try {
          qt.join(5000);
          queueThread = null;
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          // IGNORE IT
        }
      }

      closeChannel();

    } catch (Exception e) {
      // IGNORE IT
    }
  }

  public boolean enqueueMessage(final Binary message) {
    if (status == STATUS.OFFLINE)
      return false;

    return (boolean) executeInLock(new Callable() {
      @Override
      public Object call(Object iArgument) {
        // WRITE DIRECTLY TO THE MESSAGE QUEUE
        if (queue.size() > 1)
          server.getServer().log(this, Level.FINE, "Buffering request to server '%s' (status=%s buffered=%d)", remoteServerName, status, queue.size());

        if (!queue.offer(message)) {
          if (status == STATUS.OFFLINE)
            return false;

          // BACK-PRESSURE
          server.getServer()
              .log(this, Level.WARNING, "Applying back-pressure on replicating messages to server '%s' (latency=%s buffered=%d)...", getRemoteServerName(),
                  getLatencyStats(), queue.size());
          try {
            Thread.sleep(200);
          } catch (InterruptedException e) {
            // IGNORE IT
            Thread.currentThread().interrupt();
            throw new ReplicationException("Error on replicating to server '" + remoteServerName + "'");
          }

          if (status == STATUS.OFFLINE)
            return false;

          if (!queue.offer(message)) {
            server.getServer().log(this, Level.INFO, "Timeout on writing request to server '%s', setting it offline...", getRemoteServerName());

            LogManager.instance().info(this, "THREAD DUMP:\n%s", FileUtils.threadDump());

            queue.clear();
            server.setReplicaStatus(remoteServerName, false);

            // QUEUE FULL, THE REMOTE SERVER COULD BE STUCK SOMEWHERE. REMOVE THE REPLICA
            throw new ReplicationException("Replica '" + remoteServerName + "' is not reading replication messages");
          }
        }

        totalBytes += message.size();

        return true;
      }
    });
  }

  public void setStatus(final STATUS status) {
    if (this.status == status)
      // NO STATUS CHANGE
      return;

    executeInLock(new Callable() {
      @Override
      public Object call(Object iArgument) {
        Leader2ReplicaNetworkExecutor.this.status = status;
        Leader2ReplicaNetworkExecutor.this.server.getServer().log(this, Level.INFO, "Replica server '%s' is %s", remoteServerName, status);

        Leader2ReplicaNetworkExecutor.this.leftOn = status == STATUS.OFFLINE ? 0 : System.currentTimeMillis();

        if (status == STATUS.ONLINE) {
          Leader2ReplicaNetworkExecutor.this.joinedOn = System.currentTimeMillis();
          Leader2ReplicaNetworkExecutor.this.leftOn = 0;
        } else if (status == STATUS.OFFLINE) {
          Leader2ReplicaNetworkExecutor.this.leftOn = System.currentTimeMillis();
          close();
        }
        return null;
      }
    });

    if (server.getServer().isStarted())
      server.printClusterConfiguration();
  }

  public String getRemoteServerName() {
    return remoteServerName;
  }

  public String getRemoteServerAddress() {
    return remoteServerAddress;
  }

  public String getRemoteServerHTTPAddress() {
    return remoteServerHTTPAddress;
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

  public STATUS getStatus() {
    return status;
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

  public void sendMessage(final Binary msg) throws IOException {
    synchronized (channelOutputLock) {
      final ChannelBinaryServer c = channel;
      if (c == null) {
        close();
        throw new IOException("Channel closed");
      }

      c.writeBytes(msg.getContent(), msg.size());
      c.flush();
    }
  }

  @Override
  public String toString() {
    return remoteServerName;
  }

  // DO I NEED THIS?
  protected Object executeInLock(final Callable callback) {
    synchronized (lock) {
      return callback.call(null);
    }
  }
}