/*
 * Copyright (c) 2018 - Arcade Analytics LTD (https://arcadeanalytics.com)
 */

package com.arcadedb.server.ha;

import com.arcadedb.ContextConfiguration;
import com.arcadedb.GlobalConfiguration;
import com.arcadedb.database.Binary;
import com.arcadedb.exception.ConfigurationException;
import com.arcadedb.network.binary.ChannelBinaryClient;
import com.arcadedb.network.binary.QuorumNotReachedException;
import com.arcadedb.network.binary.ServerIsNotTheLeaderException;
import com.arcadedb.server.ArcadeDBServer;
import com.arcadedb.server.ServerPlugin;
import com.arcadedb.server.TestCallback;
import com.arcadedb.server.ha.message.HACommand;
import com.arcadedb.server.ha.message.HAMessageFactory;
import com.arcadedb.server.ha.message.UpdateClusterConfiguration;
import com.arcadedb.server.ha.network.DefaultServerSocketFactory;
import com.arcadedb.sql.executor.ResultInternal;
import com.arcadedb.utility.Pair;
import com.arcadedb.utility.RecordTableFormatter;
import com.arcadedb.utility.TableFormatter;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Level;

public class HAServer implements ServerPlugin {
  public enum QUORUM {
    NONE, ONE, TWO, THREE, MAJORITY, ALL
  }

  public enum ELECTION_STATUS {
    DONE, VOTING_FOR_ME, VOTING_FOR_OTHERS, LEADER_WAITING_FOR_QUORUM
  }

  private final    HAMessageFactory                           messageFactory;
  private final    ArcadeDBServer                             server;
  private final    ContextConfiguration                       configuration;
  private final    String                                     clusterName;
  private final    long                                       startedOn;
  private          int                                        configuredServers           = 1;
  private final    Map<String, Leader2ReplicaNetworkExecutor> replicaConnections          = new ConcurrentHashMap<>();
  private final    AtomicLong                                 messageNumber               = new AtomicLong(-1);
  protected final  String                                     replicationPath;
  protected        ReplicationLogFile                         replicationLogFile;
  private          Replica2LeaderNetworkExecutor              leaderConnection;
  private          LeaderNetworkListener                      listener;
  private          Map<Long, QuorumMessages>                  messagesWaitingForQuorum    = new ConcurrentHashMap<>(1024);
  private          long                                       lastConfigurationOutputHash = 0;
  private final    Object                                     sendingLock                 = new Object();
  private          String                                     serverAddress;
  private          Set<String>                                serverAddressList           = new HashSet<>();
  private          String                                     replicasHTTPAddresses;
  protected        Pair<Long, String>                         lastElectionVote;
  private volatile ELECTION_STATUS                            electionStatus              = ELECTION_STATUS.DONE;

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
    this.messageFactory = new HAMessageFactory(server);
    this.configuration = configuration;
    this.clusterName = configuration.getValueAsString(GlobalConfiguration.HA_CLUSTER_NAME);
    this.startedOn = System.currentTimeMillis();
    this.replicationPath = server.getRootPath() + "/replication";
  }

  @Override
  public void configure(final ArcadeDBServer server, final ContextConfiguration configuration) {
  }

  @Override
  public void startService() {
    final String fileName = replicationPath + "/replication_" + server.getServerName() + ".rlog";
    try {
      replicationLogFile = new ReplicationLogFile(this, fileName);
      final ReplicationMessage lastMessage = replicationLogFile.getLastMessage();
      if (lastMessage != null) {
        messageNumber.set(lastMessage.messageNumber);
        server.log(this, Level.INFO, "Found an existent replication log. Starting messages from %d", lastMessage.messageNumber);
      }
    } catch (IOException e) {
      server.log(this, Level.SEVERE, "Error on creating replication file '%s' for remote server '%s'", fileName, server.getServerName());
      stopService();
      throw new ReplicationLogException("Error on creating replication file '" + fileName + "'", e);
    }

    listener = new LeaderNetworkListener(this, new DefaultServerSocketFactory(),
        configuration.getValueAsString(GlobalConfiguration.HA_REPLICATION_INCOMING_HOST),
        configuration.getValueAsString(GlobalConfiguration.HA_REPLICATION_INCOMING_PORTS));

    serverAddress = listener.getHost() + ":" + listener.getPort();

    final String cfgServerList = configuration.getValueAsString(GlobalConfiguration.HA_SERVER_LIST).trim();
    if (!cfgServerList.isEmpty()) {
      final String[] serverEntries = cfgServerList.split(",");

      serverAddressList.clear();
      for (String serverEntry : serverEntries)
        serverAddressList.add(serverEntry);

      for (String serverEntry : serverEntries) {
        if (!serverAddress.equals(serverEntry) && connectToLeader(serverEntry)) {
          break;
        }
      }
    }

    if (leaderConnection == null)
      server.log(this, Level.INFO, "Server started as $ANSI{green Leader} in HA mode (cluster=%s)", clusterName);
  }

  @Override
  public void stopService() {
    if (listener != null)
      listener.close();

    final Replica2LeaderNetworkExecutor lc = leaderConnection;
    if (lc != null) {
      lc.close();
      leaderConnection = null;
    }

    if (!replicaConnections.isEmpty()) {
      for (Leader2ReplicaNetworkExecutor r : replicaConnections.values()) {
        r.close();
      }
      replicaConnections.clear();
    }
  }

  public void startElection() {
    final long lastReplicationMessage = replicationLogFile.getLastMessageNumber();

    long electionTurn = lastElectionVote == null ? 1 : lastElectionVote.getFirst();

    server.log(this, Level.INFO, "Starting election of local server asking for votes from %s (turn=%d lastReplicationMessage=%d)", serverAddressList,
        electionTurn, lastReplicationMessage);

    Replica2LeaderNetworkExecutor lc = leaderConnection;
    if (lc != null) {
      // CLOSE ANY LEADER CONNECTION STILL OPEN
      lc.kill();
      leaderConnection = null;
    }

    final String localServerAddress = getServerAddress();

    setElectionStatus(ELECTION_STATUS.VOTING_FOR_ME);

    while (leaderConnection == null) {
      final int majorityOfVotes = (serverAddressList.size()) / 2 + 1;

      int totalVotes = 1;

      ++electionTurn;

      lastElectionVote = new Pair<>(electionTurn, getServerName());

      for (String serverAddress : serverAddressList) {
        if (serverAddress.equals(localServerAddress))
          // SKIP LOCAL SERVER
          continue;

        try {

          final String[] parts = serverAddress.split(":");

          final ChannelBinaryClient channel = createNetworkConnection(parts[0], Integer.parseInt(parts[1]), ReplicationProtocol.COMMAND_VOTE_FOR_ME);
          channel.writeLong(electionTurn);
          channel.writeLong(lastReplicationMessage);
          channel.flush();

          final boolean vote = channel.readBoolean();
          if (vote) {
            ++totalVotes;
            server.log(this, Level.INFO, "Received the vote from server %s (turn=%d totalVotes=%d majority=%d)", serverAddress, electionTurn, totalVotes,
                majorityOfVotes);

            if (totalVotes >= majorityOfVotes)
              // AVOID CONTACTING OTHER SERVERS
              break;

          } else
            server.log(this, Level.INFO, "Did not receive the vote from server %s (turn=%d totalVotes=%d majority=%d)", serverAddress, electionTurn, totalVotes,
                majorityOfVotes);

          channel.close();
        } catch (Exception e) {
          server.log(this, Level.INFO, "Error contacting server %s for election", serverAddress);
        }
      }

      if (totalVotes >= majorityOfVotes) {
        server.log(this, Level.INFO, "Current server elected as new $ANSI{green Leader} (turn=%d totalVotes=%d majority=%d)", electionTurn, totalVotes,
            majorityOfVotes);
        sendNewLeadershipToOtherNodes();

        break;
      }

      try {
        final long timeout = 200 + new Random().nextInt(200);
        server.log(this, Level.INFO, "Not able to be elected as Leader, waiting %dms and retry", timeout);
        Thread.sleep(timeout);

        lc = leaderConnection;
        if (lc != null) {
          // I AM A REPLICA, NO LEADER ELECTION IS NEEDED
          server.log(this, Level.INFO, "Abort election process, a Leader (%s) has been already found", lc.getRemoteServerName());
          break;
        }

      } catch (InterruptedException e) {
        // INTERRUPTED
        Thread.currentThread().interrupt();
        break;
      }
    }
  }

  private void sendNewLeadershipToOtherNodes() {
    final String localServerAddress = getServerAddress();

    messageNumber.set(replicationLogFile.getLastMessageNumber());

    setElectionStatus(ELECTION_STATUS.LEADER_WAITING_FOR_QUORUM);

    server.log(this, Level.INFO, "Contacting all the servers for the new leadership (turn=%d)...", lastElectionVote.getFirst());

    for (String serverAddress : serverAddressList) {
      if (serverAddress.equals(localServerAddress))
        // SKIP LOCAL SERVER
        continue;

      try {
        final String[] parts = serverAddress.split(":");

        server.log(this, Level.INFO, "- Sending new Leader to server '%s'...", serverAddress);

        final ChannelBinaryClient channel = createNetworkConnection(parts[0], Integer.parseInt(parts[1]), ReplicationProtocol.COMMAND_ELECTION_COMPLETED);
        channel.writeLong(lastElectionVote.getFirst());
        channel.flush();

      } catch (Exception e) {
        server.log(this, Level.INFO, "Error contacting server %s for election", serverAddress);
      }
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

  public ReplicationLogFile getReplicationLogFile() {
    return replicationLogFile;
  }

  public ArcadeDBServer getServer() {
    return server;
  }

  public boolean isLeader() {
    return leaderConnection == null;
  }

  public String getLeaderName() {
    return leaderConnection == null ? getServerName() : leaderConnection.getRemoteServerName();
  }

  public Replica2LeaderNetworkExecutor getLeader() {
    return leaderConnection;
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

    final int totReplicas = replicaConnections.size();
    if (1 + totReplicas > configuredServers)
      // UPDATE SERVER COUNT
      configuredServers = 1 + totReplicas;

    if (electionStatus == ELECTION_STATUS.LEADER_WAITING_FOR_QUORUM) {
      if (1 + getOnlineReplicas() > configuredServers / 2 + 1)
        // ELECTION COMPLETED
        setElectionStatus(ELECTION_STATUS.DONE);
    }

    sendCommandToReplicas(new UpdateClusterConfiguration(getServerAddressList(), getReplicaServersHTTPAddressesList()));

    printClusterConfiguration();
  }

  public ELECTION_STATUS getElectionStatus() {
    return electionStatus;
  }

  protected void setElectionStatus(final ELECTION_STATUS status) {
    server.log(this, Level.INFO, "Change election status from %s to %s", this.electionStatus, status);
    this.electionStatus = status;
  }

  public HAMessageFactory getMessageFactory() {
    return messageFactory;
  }

  public void setServerAddresses(final String serverAddress) {
    if (serverAddress != null && !serverAddress.isEmpty()) {
      serverAddressList.clear();

      final String[] servers = serverAddress.split(",");
      for (String s : servers)
        serverAddressList.add(s);

      this.configuredServers = serverAddressList.size();
    } else
      this.configuredServers = 1;
  }

  public void sendCommandToReplicas(final HACommand command) {
    checkCurrentNodeIsTheLeader();

    final Binary buffer = new Binary();

    // SEND THE REQUEST TO ALL THE REPLICAS
    final List<Leader2ReplicaNetworkExecutor> replicas = new ArrayList<>(replicaConnections.values());

    // ASSURE THE TX ARE WRITTEN IN SEQUENCE INTO THE LOGFILE
    synchronized (sendingLock) {
      final long messageNumber = this.messageNumber.incrementAndGet();

      messageFactory.serializeCommand(command, buffer, messageNumber);

      // WRITE THE MESSAGE INTO THE LOG FIRST
      replicationLogFile.appendMessage(new ReplicationMessage(messageNumber, buffer));

      server.log(this, Level.FINE, "Sending request %d (%s) to %s", messageNumber, command, replicas);

      for (Leader2ReplicaNetworkExecutor replicaConnection : replicas) {
        // STARTING FROM THE SECOND SERVER, COPY THE BUFFER
        try {
          replicaConnection.enqueueMessage(buffer.slice(0));
        } catch (ReplicationException e) {
          // REMOVE THE REPLICA
          server.log(this, Level.SEVERE, "Replica '%s' does not respond, setting it as OFFLINE", replicaConnection.getRemoteServerName());
          setReplicaStatus(replicaConnection.getRemoteServerName(), false);
        }
      }
    }
  }

  public void sendCommandToReplicasWithQuorum(final HACommand command, final int quorum, final long timeout) {
    checkCurrentNodeIsTheLeader();

    if (quorum > 1 + getOnlineReplicas()) {
      waitAndRetryDuringElection(quorum);
      checkCurrentNodeIsTheLeader();
    }

    final Binary buffer = new Binary();

    long messageNumber = -1;

    CountDownLatch quorumSemaphore = null;

    try {
      while (true) {
        int sent = 0;

        // ASSURE THE TX ARE WRITTEN IN SEQUENCE INTO THE LOGFILE
        synchronized (sendingLock) {
          if (messageNumber == -1)
            messageNumber = this.messageNumber.incrementAndGet();

          buffer.clear();
          messageFactory.serializeCommand(command, buffer, messageNumber);

          // WRITE THE MESSAGE INTO THE LOG FIRST
          replicationLogFile.appendMessage(new ReplicationMessage(messageNumber, buffer));

          if (quorum > 1) {
            quorumSemaphore = new CountDownLatch(quorum - 1);
            // REGISTER THE REQUEST TO WAIT FOR THE QUORUM
            messagesWaitingForQuorum.put(messageNumber, new QuorumMessages(quorumSemaphore));
          }

          // SEND THE REQUEST TO ALL THE REPLICAS
          final List<Leader2ReplicaNetworkExecutor> replicas = new ArrayList<>(replicaConnections.values());

          server.log(this, Level.FINE, "Sending request %d to %s (quorum=%d)", messageNumber, replicas, quorum);

          for (Leader2ReplicaNetworkExecutor replicaConnection : replicas) {
            try {

              if (replicaConnection.enqueueMessage(buffer.slice(0)))
                ++sent;
              else {
                if (quorumSemaphore != null)
                  quorumSemaphore.countDown();
              }

            } catch (ReplicationException e) {
              server
                  .log(this, Level.SEVERE, "Error on replicating message %d to replica '%s' (error=%s)", messageNumber, replicaConnection.getRemoteServerName(),
                      e);

              // REMOVE THE REPLICA AND EXCLUDE IT FROM THE QUORUM
              if (quorumSemaphore != null)
                quorumSemaphore.countDown();
            }
          }
        }

        if (sent < quorum - 1) {
          checkCurrentNodeIsTheLeader();

          server.log(this, Level.WARNING, "Quorum " + quorum + " not reached because only " + sent + " server(s) are online");
          throw new QuorumNotReachedException("Quorum " + quorum + " not reached because only " + sent + " server(s) are online");
        }

        if (quorumSemaphore != null) {
          try {
            if (!quorumSemaphore.await(timeout, TimeUnit.MILLISECONDS)) {

              checkCurrentNodeIsTheLeader();

              if (quorum > 1 + getOnlineReplicas())
                if (waitAndRetryDuringElection(quorum))
                  continue;

              checkCurrentNodeIsTheLeader();

              server.log(this, Level.WARNING, "Timeout waiting for quorum to be reached for request " + messageNumber);
              throw new QuorumNotReachedException("Timeout waiting for quorum to be reached for request " + messageNumber);
            }

          } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new QuorumNotReachedException("Quorum not reached for request " + messageNumber + " because the thread was interrupted");
          }
        }

        // OK
        break;

      }
    } finally {
      // REQUEST IS OVER, REMOVE FROM THE QUORUM MAP
      if (quorumSemaphore != null)
        messagesWaitingForQuorum.remove(messageNumber);
    }
  }

  public void setReplicasHTTPAddresses(final String replicasHTTPAddresses) {
    this.replicasHTTPAddresses = replicasHTTPAddresses;
  }

  public String getReplicasHTTPAddresses() {
    return replicasHTTPAddresses;
  }

  public void removeServer(final String remoteServerName) {
    final Leader2ReplicaNetworkExecutor c = replicaConnections.remove(remoteServerName);
    if (c != null) {
      final RemovedServerInfo removedServer = new RemovedServerInfo(remoteServerName, c.getJoinedOn());
      server.log(this, Level.SEVERE, "Replica '%s' seems not active, removing it from the cluster", remoteServerName);
      c.close();
    }

    configuredServers = 1 + replicaConnections.size();
  }

  public int getOnlineReplicas() {
    int total = 0;
    for (Leader2ReplicaNetworkExecutor c : replicaConnections.values()) {
      if (c.getStatus() == Leader2ReplicaNetworkExecutor.STATUS.ONLINE)
        total++;
    }
    return total;
  }

  public int getConfiguredServers() {
    return configuredServers;
  }

  public List<String> getConfiguredServerNames() {
    final List<String> list = new ArrayList<>(configuredServers);
    list.add(getLeaderName());
    for (Leader2ReplicaNetworkExecutor r : replicaConnections.values())
      list.add(r.getRemoteServerName());
    return list;
  }

  public String getServerAddressList() {
    String list = getServerAddress();
    for (Leader2ReplicaNetworkExecutor r : replicaConnections.values())
      list += "," + r.getRemoteServerAddress();
    return list;
  }

  public String getReplicaServersHTTPAddressesList() {
    if (isLeader()) {
      String list = "";
      for (Leader2ReplicaNetworkExecutor r : replicaConnections.values()) {
        if (!list.isEmpty())
          list += ",";
        list += r.getRemoteServerHTTPAddress();
      }
      return list;
    }

    return replicasHTTPAddresses;
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
    line.setProperty("HOST/PORT", getServerAddress());
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
      line.setProperty("HOST/PORT", c.getRemoteServerAddress());
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

  public String getServerAddress() {
    return serverAddress;
  }

  @Override
  public String toString() {
    return getServerName();
  }

  public void resendMessagesToReplica(final long fromMessageNumber, final String replicaName) {
    // SEND THE REQUEST TO ALL THE REPLICAS
    final Leader2ReplicaNetworkExecutor replica = replicaConnections.get(replicaName);

    if (replica == null)
      throw new ReplicationException("Server '" + getServerName() + "' cannot sync replica '" + replicaName + "' because it is offline");

    synchronized (sendingLock) {

      final AtomicInteger totalSentMessages = new AtomicInteger();

      for (long pos = fromMessageNumber; pos < replicationLogFile.getSize(); ) {
        final Pair<ReplicationMessage, Long> entry = replicationLogFile.getMessage(pos);

        // STARTING FROM THE SECOND SERVER, COPY THE BUFFER
        try {
          server.log(this, Level.FINE, "Resending message %d to replica '%s'...", entry.getFirst().messageNumber, replica.getRemoteServerName());

          replica.sendMessage(entry.getFirst().payload);

          totalSentMessages.incrementAndGet();

          pos = entry.getSecond();

        } catch (Exception e) {
          // REMOVE THE REPLICA
          server.log(this, Level.SEVERE, "Replica '%s' does not respond, setting it as OFFLINE", replica.getRemoteServerName());
          setReplicaStatus(replica.getRemoteServerName(), false);
        }
      }

      server.log(this, Level.INFO, "Recovering completed. Sent %d message(s) to replica '%s'", totalSentMessages.get(), replicaName);
    }
  }

  protected boolean connectToLeader(final String serverEntry) {
    final String[] serverParts = serverEntry.split(":");
    if (serverParts.length != 2)
      throw new ConfigurationException("Found invalid server/port entry in server address '" + serverEntry + "'");

    try {
      connectToLeader(serverParts[0], Integer.parseInt(serverParts[1]));

      // OK, CONNECTED
      return true;

    } catch (ServerIsNotTheLeaderException e) {
      final String leaderAddress = e.getLeaderAddress();
      server.log(this, Level.INFO, "Remote server %s:%d is not the Leader, connecting to %s", serverParts[0], Integer.parseInt(serverParts[1]), leaderAddress);

      final String[] leader = leaderAddress.split(":");

      connectToLeader(leader[0], Integer.parseInt(leader[1]));

    } catch (Exception e) {
      server.log(this, Level.INFO, "Error on connecting to the remote Leader server %s:%d (error=%s)", serverParts[0], Integer.parseInt(serverParts[1]), e);
    }
    return false;
  }

  /**
   * Connects to a remote server. The connection succeed only if the remote server is the leader.
   */
  private void connectToLeader(final String host, final int port) {
    final Replica2LeaderNetworkExecutor lc = leaderConnection;
    if (lc != null) {
      // CLOSE ANY LEADER CONNECTION STILL OPEN
      lc.kill();
      leaderConnection = null;
    }

    // KILL ANY ACTIVE REPLICA CONNECTION
    for (Leader2ReplicaNetworkExecutor r : replicaConnections.values())
      r.close();
    replicaConnections.clear();

    leaderConnection = new Replica2LeaderNetworkExecutor(this, host, port, configuration);

    // START SEPARATE THREAD TO EXECUTE LEADER'S REQUESTS
    leaderConnection.start();
  }

  protected ChannelBinaryClient createNetworkConnection(final String host, final int port, final short commandId) throws IOException {
    final ChannelBinaryClient channel = new ChannelBinaryClient(host, port, this.configuration);

    final String clusterName = this.configuration.getValueAsString(GlobalConfiguration.HA_CLUSTER_NAME);

    // SEND SERVER INFO
    channel.writeLong(ReplicationProtocol.MAGIC_NUMBER);
    channel.writeShort(ReplicationProtocol.PROTOCOL_VERSION);
    channel.writeString(clusterName);
    channel.writeString(getServerName());
    channel.writeString(getServerAddress());
    channel.writeString(server.getHttpServer().getListeningAddress());

    channel.writeShort(commandId);
    return channel;
  }

  private boolean waitAndRetryDuringElection(final int quorum) {
    if (electionStatus == ELECTION_STATUS.DONE)
      // BLOCK HERE THE REQUEST, THE QUORUM CANNOT BE REACHED AT PRIORI
      throw new QuorumNotReachedException("Quorum " + quorum + " not reached because only " + getOnlineReplicas() + " server(s) are online");

    server.log(this, Level.INFO, "Waiting during election (quorum=%d onlineReplicas=%d)", quorum, getOnlineReplicas());

    for (int retry = 0; retry < 10 && electionStatus != ELECTION_STATUS.DONE; ++retry) {
      try {
        Thread.sleep(500);
      } catch (InterruptedException e) {
        break;
      }
    }

    server.log(this, Level.INFO, "Waiting is over (electionStatus=%s quorum=%d onlineReplicas=%d)", electionStatus, quorum, getOnlineReplicas());

    return electionStatus == ELECTION_STATUS.DONE;
  }

  private void checkCurrentNodeIsTheLeader() {
    if (!isLeader())
      throw new ServerIsNotTheLeaderException("Cannot execute command", getLeader().getRemoteServerName());
  }
}
