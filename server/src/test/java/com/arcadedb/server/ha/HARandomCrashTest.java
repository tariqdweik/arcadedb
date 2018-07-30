/*
 * Copyright (c) 2018 - Arcade Analytics LTD (https://arcadeanalytics.com)
 */

package com.arcadedb.server.ha;

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.database.Database;
import com.arcadedb.exception.DuplicatedKeyException;
import com.arcadedb.remote.RemoteDatabase;
import com.arcadedb.remote.RemoteException;
import com.arcadedb.sql.executor.Result;
import com.arcadedb.sql.executor.ResultSet;
import com.arcadedb.utility.LogManager;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Random;
import java.util.Set;
import java.util.Timer;
import java.util.TimerTask;

public class HARandomCrashTest extends ReplicationServerTest {
  private int   restarts = 0;
  private Timer timer;

  public HARandomCrashTest() {
    GlobalConfiguration.HA_QUORUM.setValue("Majority");
  }

  @Test
  public void testReplication() {
    checkDatabases();

    timer = new Timer();
    timer.schedule(new TimerTask() {
      @Override
      public void run() {
        int serverId = new Random().nextInt(getServerCount());

        if (!areAllServersOnline())
          return;

        final Database db = getLeaderServer().getDatabase(getDatabaseName());
        try {
          final long count = db.countType(VERTEX1_TYPE_NAME, true);
          if (count > (getTxs() * getVerticesPerTx()) * 9 / 10) {
            LogManager.instance()
                .info(this, "TEST: Skip stop of server because it's close to the end of the test (%d/%d)", count, getTxs() * getVerticesPerTx());
            return;
          }
        } catch (Exception e) {
          // GENERIC ERROR, SKIP STOP
          LogManager.instance().info(this, "TEST: Skip stop of server for generic error: %s", e.toString());
          return;
        } finally {
          db.close();
        }

        LogManager.instance().info(this, "TEST: Stopping the Server %s...", serverId);

        getServer(serverId).stop();
        restarts++;
        getServer(serverId).start();
      }
    }, 20000, 15000);

    final String server1Address = getServer(0).getHttpServer().getListeningAddress();
    final String[] server1AddressParts = server1Address.split(":");

    final RemoteDatabase db = new RemoteDatabase(server1AddressParts[0], Integer.parseInt(server1AddressParts[1]), getDatabaseName(), "root", "root");

    db.begin();
    try {
      LogManager.instance().info(this, "TEST: Executing %s transactions with %d vertices each...", getTxs(), getVerticesPerTx());

      long counter = 0;
      final int maxRetry = 10;

      for (int tx = 0; tx < getTxs(); ++tx) {
        for (int i = 0; i < getVerticesPerTx(); ++i) {
          for (int retry = 0; retry < 3; ++retry) {
            try {
              ResultSet resultSet = db.command("SQL", "CREATE VERTEX " + VERTEX1_TYPE_NAME + " SET id = ?, name = ?", ++counter, "distributed-test");

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
            } catch (DuplicatedKeyException e) {
              // SKIP IT
              LogManager.instance().info(this, "TEST: Safely caught DuplicatedKeyException (retry=%d/%d error=%s)", retry, maxRetry, e.toString());
              break;
            } catch (RemoteException e) {
              // IGNORE IT
              LogManager.instance().error(this, "TEST: Error on creating vertex %d, retrying (retry=%d/%d)...", e, counter, retry, maxRetry);
              try {
                Thread.sleep(500);
              } catch (InterruptedException e1) {
                Thread.currentThread().interrupt();
              }
            }
          }
        }

        db.commit();

        if (counter % 1000 == 0) {
          LogManager.instance().info(this, "TEST: - Progress %d/%d", counter, (getTxs() * getVerticesPerTx()));

          for (int i = 0; i < getServerCount(); ++i) {
            final Database database = getServerDatabase(i, getDatabaseName());
            try {
              final long tot = database.countType(VERTEX1_TYPE_NAME, false);
              LogManager.instance().info(this, "TEST: -- DB '%s' - %d records", database, tot);
            } catch (Exception e) {
              LogManager.instance().error(this, "TEST: -- ERROR ON RETRIEVING COUNT FROM DATABASE '%s'", e, database);
            } finally {
              database.close();
            }
          }

          if (isPrintingConfigurationAtEveryStep())
            getLeaderServer().getHA().printClusterConfiguration();

          LogManager.instance().flush();
        }

        db.begin();
      }

      timer.cancel();

      LogManager.instance().info(this, "Done, restarted %d times", restarts);

    } finally {
      db.close();
    }

    try {
      Thread.sleep(5000);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }

    // CHECK INDEXES ARE REPLICATED CORRECTLY
    for (int s : getServerToCheck()) {
      checkEntriesOnServer(s);
    }

    onAfterTest();

    Assertions.assertTrue(restarts >= getServerCount(), "Restarts " + restarts);
  }

  @Override
  protected int getTxs() {
    return 3000;
  }

  @Override
  protected int getVerticesPerTx() {
    return 10;
  }
}