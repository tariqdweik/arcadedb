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
import com.arcadedb.server.ha.message.*;
import com.arcadedb.server.ha.network.DefaultServerSocketFactory;
import com.arcadedb.utility.LogManager;

import java.io.FileWriter;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.logging.Level;

public class HAServer {
  private final HAMessageFactory                   messageFactory     = new HAMessageFactory(this);
  private final ArcadeDBServer                     server;
  private final ContextConfiguration               configuration;
  private final String                             clusterName;
  private final Map<String, LeaderNetworkExecutor> replicaConnections = new HashMap<>();
  private       ReplicaNetworkExecutor             leaderConnection;
  private       LeaderNetworkListener              listener;

  public HAServer(final ArcadeDBServer server, final ContextConfiguration configuration) {
    this.server = server;
    this.configuration = configuration;
    this.clusterName = configuration.getValueAsString(GlobalConfiguration.HA_CLUSTER_NAME);
  }

  public void connect() throws IOException, InterruptedException {
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

  public void close() {
    if (listener != null)
      listener.close();

    if (leaderConnection != null)
      leaderConnection.close();
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
  }

  public HAMessageFactory getMessageFactory() {
    return messageFactory;
  }

  public void sendRequestToReplicas(final Binary buffer, final HAMessage req) throws IOException {
    buffer.reset();
    req.toStream(buffer);

    buffer.flip();

    // SEND THE REQUEST TO ALL THE REPLICAS
    for (LeaderNetworkExecutor replicaConnection : replicaConnections.values()) {
      buffer.position(0);
      replicaConnection.sendRequest(buffer);
    }
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

  private void initReplica() throws IOException, InterruptedException {
    final Binary buffer = new Binary(1024);

    sendRequestToLeader(buffer, new DatabaseListRequest());
    final DatabaseListResponse databaseList = (DatabaseListResponse) receiveResponseFromLeader(buffer);

    final Set<String> databases = databaseList.getDatabases();
    for (String db : databases) {
      sendRequestToLeader(buffer, new DatabaseStructureRequest(db));
      final DatabaseStructureResponse dbStructure = (DatabaseStructureResponse) receiveResponseFromLeader(buffer);

      final Database database = server.getDatabase(db);

      installDatabase(buffer, db, dbStructure, database);
    }

    // START SEPARATE THREAD TO EXECUTE LEADER'S REQUESTS
    leaderConnection.start();
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

    while (true) {
      sendRequestToLeader(buffer, new FileContentRequest(db, fileId, from));
      final FileContentResponse fileChunk = (FileContentResponse) receiveResponseFromLeader(buffer);

      if (fileChunk.getPages() == 0)
        break;

      for (int i = 0; i < fileChunk.getPages(); ++i) {
        final ModifiablePage page = new ModifiablePage(pageManager, new PageId(file.getFileId(), from + i), pageSize);
        System.arraycopy(fileChunk.getPagesContent().getContent(), i * pageSize, page.getTrackable().getContent(), 0, pageSize);
        page.loadMetadata();
        pageManager.overridePage(page);
      }

      if (fileChunk.isLast())
        break;

      from += fileChunk.getPages();
    }
  }

  private void sendRequestToLeader(final Binary buffer, final HAMessage req) throws IOException {
    buffer.reset();
    req.toStream(buffer);

    buffer.flip();

    leaderConnection.sendRequest(buffer);
  }

  private HAMessage receiveResponseFromLeader(final Binary buffer) throws IOException {
    final byte[] response = leaderConnection.receiveResponse();
    final HAMessage message = messageFactory.getResponseMessage(response[0]);

    if (message == null) {
      LogManager.instance().info(this, "Error on reading response, message %d not valid", response[0]);
      throw new NetworkProtocolException("Error on reading response, message " + response[0] + " not valid");
    }

    buffer.reset();
    buffer.putByteArray(response);

    buffer.flip();

    message.fromStream(buffer);

    return message;
  }
}
