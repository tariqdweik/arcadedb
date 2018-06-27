/*
 * Copyright (c) 2018 - Arcade Analytics LTD (https://arcadeanalytics.com)
 */

package com.arcadedb.server.ha;

import com.arcadedb.ContextConfiguration;
import com.arcadedb.GlobalConfiguration;
import com.arcadedb.database.Binary;
import com.arcadedb.exception.ConfigurationException;
import com.arcadedb.network.binary.ServerIsNotTheLeaderException;
import com.arcadedb.server.ArcadeDBServer;
import com.arcadedb.server.ServerPlugin;
import com.arcadedb.server.TestCallback;
import com.arcadedb.server.ha.message.HACommand;
import com.arcadedb.server.ha.message.HAMessageFactory;
import com.arcadedb.server.ha.message.TxRequest;
import com.arcadedb.server.ha.network.DefaultServerSocketFactory;
import com.arcadedb.sql.executor.ResultInternal;
import com.arcadedb.utility.RecordTableFormatter;
import com.arcadedb.utility.TableFormatter;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;

public class HAServer implements ServerPlugin {
  public enum QUORUM {
    NONE, ONE, TWO, THREE, MAJORITY, ALL
  }

  private final HAMessageFactory                           messageFactory              = new HAMessageFactory();
  private final ArcadeDBServer                             server;
  private final ContextConfiguration                       configuration;
  private final String                                     clusterName;
  private final long                                       startedOn;
  private final Map<String, Leader2ReplicaNetworkExecutor> replicaConnections          = new ConcurrentHashMap<>();
  private       Replica2LeaderNetworkExecutor              leaderConnection;
  private       LeaderNetworkListener                      listener;
  private       Map<Long, QuorumMessages>                  messagesWaitingForQuorum    = new ConcurrentHashMap<>(1024);
  private       long                                       lastConfigurationOutputHash = 0;

  private class QuorumMessages {
    public final long           sentOn = System.currentTimeMillis();
    public final CountDownLatch semaphore;

    public QuorumMessages(final CountDownLatch quorumSemaphore) {
      this.semaphore = quorumSemaphore;
    }
  }

  private class RemovedServerInfo {
    String serverName;
    long   joinedOn;
    long   leftOn;

    public RemovedServerInfo(final String remoteServerName, final long joinedOn) {
      this.serverName = remoteServerName;
      this.joinedOn = joinedOn;
      this.leftOn = System.currentTimeMillis();
    }
  }

  public HAServer(final ArcadeDBServer server, final ContextConfiguration configuration) {
    this.server = server;
    this.configuration = configuration;
    this.clusterName = configuration.getValueAsString(GlobalConfiguration.HA_CLUSTER_NAME);
    this.startedOn = System.currentTimeMillis();
  }

  @Override
  public void configure(final ArcadeDBServer server, final ContextConfiguration configuration) {
  }

  @Override
  public void startService() {
    listener = new LeaderNetworkListener(this, new DefaultServerSocketFactory(),
        configuration.getValueAsString(GlobalConfiguration.HA_REPLICATION_INCOMING_HOST),
        configuration.getValueAsString(GlobalConfiguration.HA_REPLICATION_INCOMING_PORTS));

    final String localURL = listener.getHost() + ":" + listener.getPort();

    final String serverList = configuration.getValueAsString(GlobalConfiguration.HA_SERVER_LIST).trim();
    if (!serverList.isEmpty()) {
      final String[] serverEntries = serverList.split(",");
      for (String serverEntry : serverEntries) {
        if (!localURL.equals(serverEntry) && connectToLeader(serverEntry)) {
          break;
        }
      }
    }

    if (leaderConnection == null)
      server.log(this, Level.INFO, "Server started as Leader in HA mode (cluster=%s)", clusterName);
  }

  @Override
  public void stopService() {
    if (listener != null)
      listener.close();

    if (leaderConnection != null)
      leaderConnection.close();

    if (!replicaConnections.isEmpty()) {
      for (Leader2ReplicaNetworkExecutor r : replicaConnections.values()) {
        r.close();
      }
      replicaConnections.clear();
    }
  }

  public Leader2ReplicaNetworkExecutor getReplica(final String replicaName) {
    return replicaConnections.get(replicaName);
  }

  public void setReplicaStatus(final String remoteServerName, final boolean online) {
    final Leader2ReplicaNetworkExecutor c = replicaConnections.get(remoteServerName);
    if (c == null) {
      server.log(this, Level.SEVERE, "Replica '%s' was not registered", remoteServerName);
      return;
    }

    c.setStatus(online ? Leader2ReplicaNetworkExecutor.STATUS.ONLINE : Leader2ReplicaNetworkExecutor.STATUS.OFFLINE);

    server.lifecycleEvent(online ? TestCallback.TYPE.REPLICA_ONLINE : TestCallback.TYPE.REPLICA_OFFLINE, remoteServerName);
  }

  public void receivedResponseForQuorum(final String remoteServerName, final long messageNumber) {
    final long receivedOn = System.currentTimeMillis();

    final QuorumMessages msg = messagesWaitingForQuorum.get(messageNumber);
    if (msg == null)
      // QUORUM ALREADY REACHED OR TIMEOUT
      return;

    msg.semaphore.countDown();

    // UPDATE LATENCY
    final Leader2ReplicaNetworkExecutor c = replicaConnections.get(remoteServerName);
    if (c != null)
      c.updateStats(msg.sentOn, receivedOn);
  }

  public ArcadeDBServer getServer() {
    return server;
  }

  public boolean isLeader() {
    return leaderConnection == null;
  }

  public String getServerName() {
    return server.getServerName();
  }

  public String getClusterName() {
    return clusterName;
  }

  public void registerIncomingConnection(final String replicaServerName, final Leader2ReplicaNetworkExecutor connection) {
    final Leader2ReplicaNetworkExecutor previousConnection = replicaConnections.put(replicaServerName, connection);
    if (previousConnection != null) {
      // MERGE CONNECTIONS
      connection.mergeFrom(previousConnection);
    }
    printClusterConfiguration();
  }

  public HAMessageFactory getMessageFactory() {
    return messageFactory;
  }

  public void sendCommandToReplicas(final HACommand command) {
    final Binary buffer = new Binary();

    buffer.putByte(messageFactory.getCommandId(command));
    command.toStream(buffer);

    buffer.flip();

    // SEND THE REQUEST TO ALL THE REPLICAS
    final List<Leader2ReplicaNetworkExecutor> replicas = new ArrayList<>(replicaConnections.values());
    for (Leader2ReplicaNetworkExecutor replicaConnection : replicas) {
      // STARTING FROM THE SECOND SERVER, COPY THE BUFFER
      try {
        replicaConnection.enqueueMessage(buffer.slice(0));
      } catch (ReplicationException e) {
        // REMOVE THE REPLICA
        server.log(this, Level.SEVERE, "Replica '%s' does not respond, setting it as OFFLINE",
            replicaConnection.getRemoteServerName());

        setReplicaStatus(replicaConnection.getRemoteServerName(), false);
      }
    }
  }

  public int getOnlineReplicas() {
    int total = 0;
    for (Leader2ReplicaNetworkExecutor c : replicaConnections.values()) {
      if (c.getStatus() == Leader2ReplicaNetworkExecutor.STATUS.ONLINE)
        total++;
    }
    return total;
  }

  public void sendCommandToReplicasWithQuorum(final TxRequest tx, final int quorum, final long timeout) {
    final Binary buffer = new Binary();

    buffer.putByte(messageFactory.getCommandId(tx));
    tx.toStream(buffer);

    buffer.flip();

    final long txId = tx.getMessageNumber();

    CountDownLatch quorumSemaphore = null;

    try {
      if (quorum > 1) {
        quorumSemaphore = new CountDownLatch(quorum - 1);
        // REGISTER THE REQUEST TO WAIT FOR THE QUORUM
        messagesWaitingForQuorum.put(txId, new QuorumMessages(quorumSemaphore));
      }

      // SEND THE REQUEST TO ALL THE REPLICAS
      final List<Leader2ReplicaNetworkExecutor> replicas = new ArrayList<>(replicaConnections.values());
      for (Leader2ReplicaNetworkExecutor replicaConnection : replicas) {
        try {
          replicaConnection.enqueueMessage(buffer.slice(0));
        } catch (ReplicationException e) {
          server.log(this, Level.SEVERE, "Error on replicating message to replica '%s' (error=%s)",
              replicaConnection.getRemoteServerName(), e);

          // REMOVE THE REPLICA AND EXCLUDE IT FROM THE QUORUM
          if (quorumSemaphore != null)
            quorumSemaphore.countDown();
        }
      }

      if (quorumSemaphore != null) {
        try {

          if (!quorumSemaphore.await(timeout, TimeUnit.MILLISECONDS))
            throw new ReplicationException("Timeout waiting for quorum to be reached for request " + txId);

        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          throw new ReplicationException("Quorum not reached for request " + txId + " because the thread was interrupted");
        }
      }
    } finally {
      // REQUEST IS OVER, REMOVE FROM THE QUORUM MAP
      if (quorumSemaphore != null)
        messagesWaitingForQuorum.remove(txId);
    }
  }

  public void removeServer(final String remoteServerName) {
    final Leader2ReplicaNetworkExecutor c = replicaConnections.remove(remoteServerName);
    if (c != null) {
      final RemovedServerInfo removedServer = new RemovedServerInfo(remoteServerName, c.getJoinedOn());
      server.log(this, Level.SEVERE, "Replica '%s' seems not active, removing it from the cluster", remoteServerName);
      c.close();
    }
  }

  public void printClusterConfiguration() {
    final StringBuilder buffer = new StringBuilder("NEW CLUSTER CONFIGURATION\n");
    final TableFormatter table = new TableFormatter(new TableFormatter.OTableOutput() {
      @Override
      public void onMessage(final String text, final Object... args) {
        buffer.append(String.format(text, args));
      }
    });

    final List<RecordTableFormatter.PTableRecordRow> list = new ArrayList<>();

    ResultInternal line = new ResultInternal();
    list.add(new RecordTableFormatter.PTableRecordRow(line));

    line.setProperty("SERVER", getServerName());
    line.setProperty("ROLE", "Leader");
    line.setProperty("STATUS", "ONLINE");
    line.setProperty("JOINED ON", new Date(startedOn));
    line.setProperty("LEFT ON", "");
    line.setProperty("THROUGHPUT", "");
    line.setProperty("LATENCY", "");

    for (Leader2ReplicaNetworkExecutor c : replicaConnections.values()) {
      line = new ResultInternal();
      list.add(new RecordTableFormatter.PTableRecordRow(line));

      final Leader2ReplicaNetworkExecutor.STATUS status = c.getStatus();

      line.setProperty("SERVER", c.getRemoteServerName());
      line.setProperty("ROLE", "Replica");
      line.setProperty("STATUS", status);
      line.setProperty("JOINED ON", c.getJoinedOn() > 0 ? new Date(c.getJoinedOn()) : "");
      line.setProperty("LEFT ON", c.getLeftOn() > 0 ? new Date(c.getLeftOn()) : "");
      line.setProperty("THROUGHPUT", c.getThroughputStats());
      line.setProperty("LATENCY", c.getLatencyStats());
    }

    table.writeRows(list, -1);

    final String output = buffer.toString();

    int hash = 7;
    for (int i = 0; i < output.length(); i++)
      hash = hash * 31 + output.charAt(i);

    if (lastConfigurationOutputHash == hash)
      // NO CHANGES, AVOID PRINTING CFG
      return;

    lastConfigurationOutputHash = hash;

    server.log(this, Level.INFO, output + "\n");
  }

  @Override
  public String toString() {
    return getServerName();
  }

  private boolean connectToLeader(final String serverEntry) {
    final String[] serverParts = serverEntry.split(":");
    if (serverParts.length != 2)
      throw new ConfigurationException(
          "Found invalid server/port entry in " + GlobalConfiguration.HA_SERVER_LIST.getKey() + " setting: " + serverEntry);

    try {
      connectToLeader(serverParts[0], Integer.parseInt(serverParts[1]));

      // OK, CONNECTED
      return true;

    } catch (ServerIsNotTheLeaderException e) {
      // TODO: SET THIS LOG TO DEBUG
      server.log(this, Level.INFO, "Remote server %s:%d is not the leader, connecting to %s", serverParts[0],
          Integer.parseInt(serverParts[1]), e.getLeaderURL());

    } catch (Exception e) {
      server.log(this, Level.INFO, "Error on connecting to the remote server %s:%d (error=%s)", serverParts[0],
          Integer.parseInt(serverParts[1]), e);
    }
    return false;
  }

  /**
   * Connects to a remote server. The connection succeed only if the remote server is the leader.
   */
  private void connectToLeader(final String host, final int port) throws IOException {
    leaderConnection = new Replica2LeaderNetworkExecutor(this, host, port, configuration);

    // START SEPARATE THREAD TO EXECUTE LEADER'S REQUESTS
    leaderConnection.start();
  }
}
