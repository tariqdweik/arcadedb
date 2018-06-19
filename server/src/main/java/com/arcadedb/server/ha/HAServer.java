/*
 * Copyright (c) 2018 - Arcade Analytics LTD (https://arcadeanalytics.com)
 */

package com.arcadedb.server.ha;

import com.arcadedb.ContextConfiguration;
import com.arcadedb.GlobalConfiguration;
import com.arcadedb.database.Binary;
import com.arcadedb.database.Database;
import com.arcadedb.engine.ModifiablePage;
import com.arcadedb.engine.PageId;
import com.arcadedb.engine.PageManager;
import com.arcadedb.engine.PaginatedFile;
import com.arcadedb.exception.ConfigurationException;
import com.arcadedb.network.binary.ChannelBinaryClient;
import com.arcadedb.network.binary.ConnectionException;
import com.arcadedb.network.binary.NetworkProtocolException;
import com.arcadedb.network.binary.ServerIsNotTheLeaderException;
import com.arcadedb.schema.SchemaImpl;
import com.arcadedb.server.ArcadeDBServer;
import com.arcadedb.server.ServerException;
import com.arcadedb.server.ServerPlugin;
import com.arcadedb.server.ha.message.*;
import com.arcadedb.server.ha.network.DefaultServerSocketFactory;
import com.arcadedb.sql.executor.ResultInternal;
import com.arcadedb.utility.FileUtils;
import com.arcadedb.utility.LogManager;
import com.arcadedb.utility.RecordTableFormatter;
import com.arcadedb.utility.TableFormatter;

import java.io.FileWriter;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;

public class HAServer implements ServerPlugin {
  public enum QUORUM {
    NONE, ONE, TWO, THREE, MAJORITY, ALL
  }

  private final HAMessageFactory                   messageFactory           = new HAMessageFactory();
  private final ArcadeDBServer                     server;
  private final ContextConfiguration               configuration;
  private final String                             clusterName;
  private final long                               startedOn;
  private final Map<String, LeaderNetworkExecutor> replicaConnections       = new ConcurrentHashMap<>();
  private       ReplicaNetworkExecutor             leaderConnection;
  private       LeaderNetworkListener              listener;
  private       Map<Long, QuorumMessages>          messagesWaitingForQuorum = new ConcurrentHashMap<>(1024);

  private class QuorumMessages {
    public final long           sentOn = System.currentTimeMillis();
    public final CountDownLatch semaphore;

    public QuorumMessages(final CountDownLatch quorumSemaphore) {
      this.semaphore = quorumSemaphore;
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
        if (!localURL.equals(serverEntry) && connectTo(serverEntry)) {
          break;
        }
      }
    }

    if (leaderConnection != null) {
      server.log(this, Level.INFO, "Server started as Replica in HA mode (cluster=%s leader=%s)", clusterName,
          leaderConnection.getURL());
      initReplica();
    } else {
      server.log(this, Level.INFO, "Server started as Leader in HA mode (cluster=%s)", clusterName);
    }
  }

  @Override
  public void stopService() {
    if (listener != null)
      listener.close();

    if (leaderConnection != null)
      leaderConnection.close();

    if (!replicaConnections.isEmpty()) {
      for (LeaderNetworkExecutor r : replicaConnections.values()) {
        r.close();
      }
    }
  }

  public void receivedResponseForQuorum(final String remoteServerName, final long messageNumber) {
    final long receivedOn = System.currentTimeMillis();

    final QuorumMessages msg = messagesWaitingForQuorum.get(messageNumber);
    if (msg == null)
      // QUORUM ALREADY REACHED OR TIMEOUT
      return;

    msg.semaphore.countDown();

    // UPDATE LATENCY
    final LeaderNetworkExecutor c = replicaConnections.get(remoteServerName);
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

  public void registerIncomingConnection(final String replicaServerName, final LeaderNetworkExecutor connection) {
    replicaConnections.put(replicaServerName, connection);
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
    final List<LeaderNetworkExecutor> replicas = new ArrayList<>(replicaConnections.values());
    for (LeaderNetworkExecutor replicaConnection : replicas) {
      // STARTING FROM THE SECOND SERVER, COPY THE BUFFER
      try {
        replicaConnection.enqueueMessage(buffer.slice(0));
      } catch (ReplicationException e) {
        // REMOVE THE REPLICA
        server.log(this, Level.SEVERE, "Replica '%s' does not respond, removing it from the cluster",
            replicaConnection.getRemoteServerName());

        removeServer(replicaConnection.getRemoteServerName());
      }
    }
  }

  public int getOnlineReplicas() {
    return replicaConnections.size();
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
      final List<LeaderNetworkExecutor> replicas = new ArrayList<>(replicaConnections.values());
      for (LeaderNetworkExecutor replicaConnection : replicas) {
        try {
          replicaConnection.enqueueMessage(buffer.slice(0));
        } catch (ReplicationException e) {
          server.log(this, Level.SEVERE, "Replica '%s' does not respond, removing it from the cluster",
              replicaConnection.getRemoteServerName());

          // REMOVE THE REPLICA AND EXCLUDE IT FROM THE QUORUM
          if (quorumSemaphore != null)
            quorumSemaphore.countDown();

          removeServer(replicaConnection.getRemoteServerName());
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
    final LeaderNetworkExecutor c = replicaConnections.remove(remoteServerName);
    if (c != null) {
      server.log(this, Level.SEVERE, "Replica '%s' seems not active, removing it from the cluster", remoteServerName);
      c.close();
    }
  }

  public void printClusterConfiguration() {
    server.log(this, Level.INFO, "NEW CLUSTER CONFIGURATION");

    final StringBuilder buffer = new StringBuilder();
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
    line.setProperty("STATUS", "UP");
    line.setProperty("JOINED ON", new Date(startedOn));
    line.setProperty("THROUGHPUT", "");
    line.setProperty("LATENCY", "");

    for (LeaderNetworkExecutor c : replicaConnections.values()) {
      line = new ResultInternal();
      list.add(new RecordTableFormatter.PTableRecordRow(line));

      line.setProperty("SERVER", c.getRemoteServerName());
      line.setProperty("ROLE", "Replica");
      line.setProperty("STATUS", "UP");
      line.setProperty("JOINED ON", new Date(c.getJoinedOn()));
      line.setProperty("THROUGHPUT", c.getThroughputStats());
      line.setProperty("LATENCY", c.getLatencyStats());
    }

    table.writeRows(list, -1);

    server.log(this, Level.INFO, buffer.toString() + "\n");
  }

  private boolean connectTo(final String serverEntry) {
    final String[] serverParts = serverEntry.split(":");
    if (serverParts.length != 2)
      throw new ConfigurationException(
          "Found invalid server/port entry in " + GlobalConfiguration.HA_SERVER_LIST.getKey() + " setting: " + serverEntry);

    try {
      connectTo(serverParts[0], Integer.parseInt(serverParts[1]));

      // OK, CONNECTED
      return true;

    } catch (ServerIsNotTheLeaderException e) {
      // TODO: SET THIS LOG TO DEBUG
      server.log(this, Level.INFO, "Remote server %s:%d is not the leader, connecting to %s", serverParts[0],
          Integer.parseInt(serverParts[1]), e.getLeaderURL());

    } catch (Exception e) {
      server.log(this, Level.INFO, "Error on connecting to the remote server %s:%d", serverParts[0],
          Integer.parseInt(serverParts[1]));
    }
    return false;
  }

  /**
   * Connects to a remote server. The connection succeed only if the remote server is the leader.
   */
  private void connectTo(final String host, final int port) throws IOException {
    server.log(this, Level.INFO, "Connecting to server %s:%d...", host, port);

    final ChannelBinaryClient client = new ChannelBinaryClient(host, port, configuration);

    // SEND SERVER INFO
    client.writeShort((short) LeaderNetworkExecutor.PROTOCOL_VERSION);
    client.writeString(configuration.getValueAsString(GlobalConfiguration.HA_CLUSTER_NAME));
    client.writeString(configuration.getValueAsString(GlobalConfiguration.SERVER_NAME));
    client.flush();

    // READ RESPONSE
    final boolean connectionAccepted = client.readBoolean();
    if (!connectionAccepted) {
      final String reason = client.readString();
      client.close();
      throw new ConnectionException(host + ":" + port, reason);
    }

    server.log(this, Level.INFO, "Server connected to the server leader %s:%d", host, port);

    leaderConnection = new ReplicaNetworkExecutor(this, client);
  }

  private void initReplica() {
    final Binary buffer = new Binary(1024);

    try {
      leaderConnection.sendCommandToLeader(buffer, new DatabaseListRequest());
      final DatabaseListResponse databaseList = (DatabaseListResponse) receiveCommandFromLeader(buffer);

      final Set<String> databases = databaseList.getDatabases();
      for (String db : databases) {
        leaderConnection.sendCommandToLeader(buffer, new DatabaseStructureRequest(db));
        final DatabaseStructureResponse dbStructure = (DatabaseStructureResponse) receiveCommandFromLeader(buffer);

        final Database database = server.getOrCreateDatabase(db);

        installDatabase(buffer, db, dbStructure, database);
      }

      // START SEPARATE THREAD TO EXECUTE LEADER'S REQUESTS
      leaderConnection.start();
    } catch (Exception e) {
      throw new ServerException("Cannot start HA service", e);
    }
  }

  private void installDatabase(final Binary buffer, final String db, final DatabaseStructureResponse dbStructure,
      final Database database) throws IOException {

    // WRITE THE SCHEMA
    final FileWriter schemaFile = new FileWriter(database.getDatabasePath() + "/" + SchemaImpl.SCHEMA_FILE_NAME);
    try {
      schemaFile.write(dbStructure.getSchemaJson());
    } finally {
      schemaFile.close();
    }

    // WRITE ALL THE FILES
    for (Map.Entry<Integer, String> f : dbStructure.getFileNames().entrySet()) {
      installFile(buffer, db, database, f.getKey(), f.getValue());
    }

    // RELOAD THE SCHEMA
    ((SchemaImpl) database.getSchema()).close();
    ((SchemaImpl) database.getSchema()).load(PaginatedFile.MODE.READ_ONLY);
  }

  private void installFile(final Binary buffer, final String db, final Database database, final int fileId, final String fileName)
      throws IOException {
    final PageManager pageManager = database.getPageManager();

    final PaginatedFile file = database.getFileManager().getOrCreateFile(fileId, database.getDatabasePath() + "/" + fileName);

    final int pageSize = file.getPageSize();

    int from = 0;

    server.log(this, Level.FINE, "Installing file '%s'...", fileName);

    int pages = 0;
    long fileSize = 0;

    while (true) {
      leaderConnection.sendCommandToLeader(buffer, new FileContentRequest(db, fileId, from));
      final FileContentResponse fileChunk = (FileContentResponse) receiveCommandFromLeader(buffer);

      if (fileChunk.getPages() == 0)
        break;

      for (int i = 0; i < fileChunk.getPages(); ++i) {
        final ModifiablePage page = new ModifiablePage(pageManager, new PageId(file.getFileId(), from + i), pageSize);
        System.arraycopy(fileChunk.getPagesContent().getContent(), i * pageSize, page.getTrackable().getContent(), 0, pageSize);
        page.loadMetadata();
        pageManager.overridePage(page);

        ++pages;
        fileSize += pageSize;
      }

      if (fileChunk.isLast())
        break;

      from += fileChunk.getPages();
    }

    server.log(this, Level.FINE, "File '%s' installed (pages=%d size=%s)", fileName, pages, FileUtils.getSizeAsString(fileSize));
  }

  private HACommand receiveCommandFromLeader(final Binary buffer) throws IOException {
    final byte[] response = leaderConnection.receiveResponse();
    final HACommand message = messageFactory.getCommand(response[0]);

    if (message == null) {
      LogManager.instance().info(this, "Error on reading response, message %d not valid", response[0]);
      throw new NetworkProtocolException("Error on reading response, message " + response[0] + " not valid");
    }

    buffer.reset();
    buffer.putByteArray(response);

    buffer.flip();

    // SKIP COMMAND ID
    buffer.getByte();

    message.fromStream(buffer);

    return message;
  }
}
