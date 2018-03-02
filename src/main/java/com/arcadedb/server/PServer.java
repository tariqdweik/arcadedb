package com.arcadedb.server;

import com.arcadedb.utility.PLogManager;
import kafka.server.KafkaConfig;
import kafka.server.KafkaServer;
import kafka.utils.SystemTime$;
import org.apache.log4j.ConsoleAppender;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.PatternLayout;
import org.apache.zookeeper.server.ServerCnxnFactory;
import org.apache.zookeeper.server.ServerConfig;
import org.apache.zookeeper.server.ZooKeeperServer;
import org.apache.zookeeper.server.persistence.FileTxnSnapLog;
import org.apache.zookeeper.server.quorum.QuorumPeerConfig;

import java.io.File;
import java.io.IOException;
import java.util.Properties;

public class PServer {
  private ServerCnxnFactory zooKeeperCnxnFactory;
  private volatile boolean kafkaStarted     = false;
  private volatile boolean zooKeeperStarted = false;
  private KafkaServer kafkaServer;

  public static void main(final String args[]) throws Exception {
    new PServer().start("Server1", "localhost", 2482);
  }

  public PServer() {
    // DEFAULT LOG SETTING
    Logger root = Logger.getRootLogger();
    root.setLevel(Level.INFO);
    root.addAppender(new ConsoleAppender(new PatternLayout(PatternLayout.TTCC_CONVERSION_PATTERN)));
  }

  public void start(final String serverName, final String hostName, final int hostPort) throws Exception {
    PLogManager.instance().info(this, "Starting Proton Server...");

    startZookeeper();

    final Properties brokerConfig = new Properties();

    brokerConfig.setProperty("broker.id", "1");
    brokerConfig.setProperty("host.name", hostName);
    brokerConfig.setProperty("port", String.valueOf(hostPort));
    brokerConfig.setProperty("zookeeper.connect", hostName + ":" + String.valueOf(hostPort + 1));

    final KafkaConfig cfg = new KafkaConfig(brokerConfig);

    PLogManager.instance().debug(this, "Starting Kafka service...");

    try {
      kafkaServer = new KafkaServer(cfg, SystemTime$.MODULE$);
      kafkaServer.startup();
      kafkaStarted = true;
      PLogManager.instance().debug(this, "Kafka service started");
    } catch (Exception e) {
      PLogManager.instance().error(this, "Error on starting Kafka server", e);
      if (kafkaServer != null) {
        kafkaServer.shutdown();
        kafkaServer = null;
      }
    }

    PLogManager.instance().info(this, "Proton Server started");

    kafkaServer.awaitShutdown();
  }

  private void startZookeeper() {
    // IGNORE LOG4J WARNING
    System.setProperty("log4j.defaultInitOverride", "true");

    final Properties startupProperties = new Properties();
    startupProperties.setProperty("clientPort", "2483");
    startupProperties.setProperty("dataDir", "./logs");
    startupProperties.setProperty("tickTime", "2");

    QuorumPeerConfig quorumConfiguration = new QuorumPeerConfig();
    try {
      quorumConfiguration.parseProperties(startupProperties);
    } catch (Exception e) {
      throw new PServerException("Error on starting ZooKeeper. Configuration not valid", e);
    }

    final ServerConfig config = new ServerConfig();
    config.readFrom(quorumConfiguration);

    new Thread() {
      public void run() {
        try {
          PLogManager.instance().debug(this, "Starting Zookeeper service...");

          FileTxnSnapLog txnLog = null;
          try {
            ZooKeeperServer zkServer = new ZooKeeperServer();

            txnLog = new FileTxnSnapLog(new File(config.getDataLogDir()), new File(config.getDataDir()));
            zkServer.setTxnLogFactory(txnLog);
            zkServer.setTickTime(config.getTickTime());
            zkServer.setMinSessionTimeout(config.getMinSessionTimeout());
            zkServer.setMaxSessionTimeout(config.getMaxSessionTimeout());
            zooKeeperCnxnFactory = ServerCnxnFactory.createFactory();
            zooKeeperCnxnFactory.configure(config.getClientPortAddress(), config.getMaxClientCnxns());
            zooKeeperCnxnFactory.startup(zkServer);

            zooKeeperStarted = true;
            PLogManager.instance().debug(this, "Zookeeper service is started");

            zooKeeperCnxnFactory.join();
            if (zkServer.isRunning()) {
              zkServer.shutdown();
            }
          } catch (InterruptedException e) {
            // warn, but generally this is ok
            PLogManager.instance().error(this, "Zookeeper service interrupted", e);
          } finally {
            if (txnLog != null) {
              txnLog.close();
            }
          }
        } catch (IOException e) {
          PLogManager.instance().error(this, "Error on starting ZooKeeper", e);
          if (zooKeeperCnxnFactory != null)
            zooKeeperCnxnFactory.shutdown();
        }
      }
    }.start();

    // WAIT FOR ZOOKEEPER TO START
    for (int retry = 0; !zooKeeperStarted && retry < 10; retry++)
      try {
        Thread.sleep(100);
      } catch (InterruptedException e) {
        stopZookeeper();
        PLogManager.instance().error(this, "Error on starting ZooKeeper", e);
        throw new PServerException("Error on starting ZooKeeper", e);
      }

    if (!zooKeeperStarted) {
      stopZookeeper();
      PLogManager.instance().info(this, "Timeout on starting ZooKeeper");
      throw new PServerException("Timeout on starting ZooKeeper");
    }
  }

  private void stopZookeeper() {
    if (zooKeeperCnxnFactory != null)
      zooKeeperCnxnFactory.shutdown();
  }
}
