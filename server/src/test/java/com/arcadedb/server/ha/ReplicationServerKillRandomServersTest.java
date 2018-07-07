/*
 * Copyright (c) 2018 - Arcade Analytics LTD (https://arcadeanalytics.com)
 */

package com.arcadedb.server.ha;

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.remote.RemoteDatabase;
import com.arcadedb.remote.RemoteException;
import com.arcadedb.server.ArcadeDBServer;
import com.arcadedb.sql.executor.Result;
import com.arcadedb.sql.executor.ResultSet;
import com.arcadedb.utility.LogManager;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Set;
import java.util.Timer;
import java.util.TimerTask;

public class ReplicationServerKillRandomServersTest extends ReplicationServerTest {
  private int   restarts = 0;
  private Timer timer;

  public ReplicationServerKillRandomServersTest() {
    GlobalConfiguration.HA_QUORUM.setValue("Majority");
  }

  @Test
  public void testReplication() {
    checkDatabases();

    timer = new Timer();
    timer.schedule(new TimerTask() {
      @Override
      public void run() {
        ArcadeDBServer onlineServer = null;
        for (int i = 0; i < getServerCount(); ++i)
          if (getServer(i).isStarted()) {
            onlineServer = getServer(i);
            break;
          }

        Assertions.assertNotNull(onlineServer);

        final String leaderName = onlineServer.getHA().getLeaderName();

        for (int i = 0; i < getServerCount(); ++i)
          if (!getServer(i).isStarted()) {
            // NOT ALL THE SERVERS ARE UP, AVOID A QUORUM ERROR
            LogManager.instance().info(this, "TEST: Not all the servers are ONLINE, skip this crash...");
            return;
          }

        LogManager.instance().info(this, "TEST: Stopping the Leader %s...", leaderName);

        getServer(leaderName).stop();
        restarts++;
        getServer(leaderName).start();
      }
    }, 30000, 30000);

    final String server1Address = getServer(0).getHttpServer().getListeningAddress();
    final String[] server1AddressParts = server1Address.split(":");

    final RemoteDatabase db = new RemoteDatabase(server1AddressParts[0], Integer.parseInt(server1AddressParts[1]), getDatabaseName(), "root", "root");

    db.begin();
    try

    {
      LogManager.instance().info(this, "Executing %s transactions with %d vertices each...", getTxs(), getVerticesPerTx());

      long counter = 0;

      for (int tx = 0; tx < getTxs(); ++tx) {
        for (int i = 0; i < getVerticesPerTx(); ++i) {
          for (int retry = 0; retry < 3; ++retry) {
            try {
              ResultSet resultSet = db.sql("CREATE VERTEX " + VERTEX1_TYPE_NAME + " SET id = ?, name = ?", ++counter, "distributed-test");

              Assertions.assertTrue(resultSet.hasNext());
              final Result result = resultSet.next();
              Assertions.assertNotNull(result);
              final Set<String> props = result.getPropertyNames();
              Assertions.assertEquals(2, props.size());
              Assertions.assertTrue(props.contains("id"));
              Assertions.assertEquals(counter, (int) result.getProperty("id"));
              Assertions.assertTrue(props.contains("name"));
              Assertions.assertEquals("distributed-test", result.getProperty("name"));
              break;
            } catch (RemoteException e) {
              // IGNORE IT
              LogManager.instance().error(this, "Error on creating vertex %d, retrying...", e, counter);
            }
          }
        }

        db.commit();

        if (counter % 1000 == 0) {
          LogManager.instance().info(this, "- Progress %d/%d", counter, (getTxs() * getVerticesPerTx()));
          if (isPrintingConfigurationAtEveryStep())
            getServer(0).getHA().printClusterConfiguration();
        }

        db.begin();
      }

      timer.cancel();

      LogManager.instance().info(this, "Done");

    } finally {
      db.close();
    }

    try {
      Thread.sleep(1000);
    } catch (InterruptedException e) {
    }

    // CHECK INDEXES ARE REPLICATED CORRECTLY
    for (int s : getServerToCheck()) {
      checkEntriesOnServer(s);
    }

    onAfterTest();

    Assertions.assertTrue(restarts >= getServerCount());
  }

  @Override
  protected boolean isPrintingConfigurationAtEveryStep() {
    return false;
  }

  @Override
  protected int getTxs() {
    return 50000;
  }

  @Override
  protected int getVerticesPerTx() {
    return 10;
  }
}