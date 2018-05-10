/*
 * Copyright (c) 2018 - Arcade Analytics LTD (https://arcadeanalytics.com)
 */

package com.arcadedb.server.ha;

import com.arcadedb.ContextConfiguration;
import com.arcadedb.GlobalConfiguration;
import com.arcadedb.server.ServerException;
import com.arcadedb.utility.LogManager;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.admin.ZooKeeperAdmin;
import org.apache.zookeeper.data.Stat;
import org.apache.zookeeper.server.ServerCnxnFactory;
import org.apache.zookeeper.server.ServerConfig;
import org.apache.zookeeper.server.ZooKeeperServer;
import org.apache.zookeeper.server.persistence.FileTxnSnapLog;
import org.apache.zookeeper.server.quorum.QuorumPeerConfig;

import java.io.IOException;
import java.net.BindException;
import java.util.List;
import java.util.Properties;

public class HAServer {
  private final ContextConfiguration configuration;
  private       ZooKeeperServer      zkServer;
  private       ServerCnxnFactory    zooKeeperCnxnFactory;
  private       boolean              zooKeeperStarted;

  public HAServer(final ContextConfiguration configuration) {
    this.configuration = configuration;
  }

  public boolean isZooKeeperStarted() {
    return zooKeeperStarted;
  }

  public void stop() {
    LogManager.instance().info(this, "- Shutting down ZooKeeper Server...");

    if (zkServer != null)
      if (zkServer.isRunning())
        zkServer.shutdown();

    if (zooKeeperCnxnFactory != null) {
      try {
        zooKeeperCnxnFactory.shutdown();
        zooKeeperCnxnFactory.join();
      } catch (InterruptedException e) {
        // IGNORE IT
      }
    }

    LogManager.instance().info(this, "- ZooKeeper is down");
  }

  public void start() throws IOException {
    // IGNORE LOG4J WARNING
    System.setProperty("log4j.defaultInitOverride", "true");

    int port = configuration.getValueAsInteger(GlobalConfiguration.SERVER_ZOOKEEPER_PORT);

    final boolean zooKeeperAutoIncrementPort = configuration
        .getValueAsBoolean(GlobalConfiguration.SERVER_ZOOKEEPER_AUTOINCREMENT_PORT);

    do {
      final Properties startupProperties = new Properties();
      startupProperties.setProperty("clientPort", "" + port);
      startupProperties.setProperty("dataDir", "./logs");
      //startupProperties.setProperty("standaloneEnabled", "false");
      startupProperties.setProperty("reconfigEnabled", "true");
      startupProperties.setProperty("tickTime", "2");

      QuorumPeerConfig quorumConfiguration = new QuorumPeerConfig();
      try {
        quorumConfiguration.parseProperties(startupProperties);
      } catch (Exception e) {
        throw new ServerException("Error on starting ZooKeeper. Configuration not valid", e);
      }

      final ServerConfig config = new ServerConfig();
      config.readFrom(quorumConfiguration);

      FileTxnSnapLog txnLog = null;
      try {

        LogManager.instance().debug(this, "- Starting ZooKeeper service...");

        zkServer = new ZooKeeperServer();

        txnLog = new FileTxnSnapLog(config.getDataLogDir(), config.getDataDir());
        zkServer.setTxnLogFactory(txnLog);
        zkServer.setTickTime(config.getTickTime());
        zkServer.setMinSessionTimeout(config.getMinSessionTimeout());
        zkServer.setMaxSessionTimeout(config.getMaxSessionTimeout());
        zooKeeperCnxnFactory = ServerCnxnFactory.createFactory();
        zooKeeperCnxnFactory.configure(config.getClientPortAddress(), config.getMaxClientCnxns());
        zooKeeperCnxnFactory.startup(zkServer);

        zooKeeperStarted = true;
        LogManager.instance().info(this, "- ZooKeeper service started (port=%d)", port);
        break;

      } catch (BindException e) {
        LogManager.instance().warn(this, "- ZooKeeper Port %d not available", port);
        // RETRY
        ++port;

      } catch (InterruptedException e) {
        LogManager.instance().warn(this, "Zookeeper service interrupted", e);
        break;
      } catch (Exception e) {
        LogManager.instance().error(this, "Error on starting ZooKeeper", e);
        break;
      }
    } while (zooKeeperAutoIncrementPort);

    if (!zooKeeperStarted) {
      stop();
      return;
    }
  }

  public byte[] reconfig(List<String> joiningServers, List<String> leavingServers) {
    try {
      final ZooKeeperAdmin admin = new ZooKeeperAdmin(
          "localhost:" + configuration.getValueAsInteger(GlobalConfiguration.SERVER_ZOOKEEPER_PORT), 5000, new Watcher() {
        @Override
        public void process(WatchedEvent watchedEvent) {
          LogManager.instance().info(this, "HA - Received event %s", watchedEvent);
        }
      });

      return admin.reconfigure(joiningServers, leavingServers, null, 0, new Stat());

    } catch (IOException e) {
      e.printStackTrace();
    } catch (InterruptedException e) {
      e.printStackTrace();
    } catch (KeeperException e) {
      e.printStackTrace();
    }
    return null;
  }
}
