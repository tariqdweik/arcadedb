/*
 * Copyright (c) - Arcade Data LTD (https://arcadedata.com)
 */

package com.arcadedb.server.ha;

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.exception.DuplicatedKeyException;
import com.arcadedb.exception.NeedRetryException;
import com.arcadedb.exception.TimeoutException;
import com.arcadedb.exception.TransactionException;
import com.arcadedb.log.LogManager;
import com.arcadedb.remote.RemoteDatabase;
import com.arcadedb.server.ArcadeDBServer;
import com.arcadedb.server.TestCallback;
import com.arcadedb.server.ha.message.TxRequest;
import com.arcadedb.sql.executor.Result;
import com.arcadedb.sql.executor.ResultSet;
import com.arcadedb.utility.Pair;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;

public class ReplicationServerLeaderChanges3TimesTest extends ReplicationServerTest {
  private final AtomicInteger                       messagesInTotal    = new AtomicInteger();
  private final AtomicInteger                       messagesPerRestart = new AtomicInteger();
  private       AtomicInteger                       restarts           = new AtomicInteger();
  private       ConcurrentHashMap<Integer, Boolean> semaphore          = new ConcurrentHashMap<>();

  public ReplicationServerLeaderChanges3TimesTest() {
    GlobalConfiguration.HA_QUORUM.setValue("Majority");
  }

  @Test
  public void testReplication() {
    checkDatabases();

    final String server1Address = getServer(0).getHttpServer().getListeningAddress();
    final String[] server1AddressParts = server1Address.split(":");

    final RemoteDatabase db = new RemoteDatabase(server1AddressParts[0], Integer.parseInt(server1AddressParts[1]), getDatabaseName(), "root", "root");

    db.begin();

    LogManager.instance().log(this, Level.INFO, "Executing %s transactions with %d vertices each...", null, getTxs(), getVerticesPerTx());

    long counter = 0;
    final int maxRetry = 10;

    for (int tx = 0; tx < getTxs(); ++tx) {
      for (int retry = 0; retry < 3; ++retry) {
        try {
          for (int i = 0; i < getVerticesPerTx(); ++i) {
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

          }

          db.commit();

        } catch (DuplicatedKeyException | NeedRetryException | TimeoutException | TransactionException e) {
          // IGNORE IT
          LogManager.instance().log(this, Level.SEVERE, "Error on creating vertex %d, retrying (retry=%d/%d)...", e, counter, retry, maxRetry);
          try {
            Thread.sleep(500);
          } catch (InterruptedException e1) {
            Thread.currentThread().interrupt();
          }
        } catch (Exception e) {
          // IGNORE IT
          LogManager.instance().log(this, Level.SEVERE, "Generic Exception", e);
        }
      }

      if (counter % 1000 == 0) {
        LogManager.instance().log(this, Level.INFO, "- Progress %d/%d", null, counter, (getTxs() * getVerticesPerTx()));
        if (isPrintingConfigurationAtEveryStep())
          getLeaderServer().getHA().printClusterConfiguration();
      }

      db.begin();
    }

    LogManager.instance().log(this, Level.INFO, "Done");

    try {
      Thread.sleep(1000);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }

    // CHECK INDEXES ARE REPLICATED CORRECTLY
    for (int s : getServerToCheck()) {
      checkEntriesOnServer(s);
    }

    onAfterTest();

    LogManager.instance().log(this, Level.INFO, "TEST Restart = %d", null, restarts);
    Assertions.assertTrue(restarts.get() >= getServerCount());
  }

  @Override
  protected void onBeforeStarting(final ArcadeDBServer server) {
    server.registerTestEventListener(new TestCallback() {
      @Override
      public void onEvent(final TYPE type, final Object object, final ArcadeDBServer server) {
        if (type == TYPE.REPLICA_MSG_RECEIVED) {
          if (!(((Pair) object).getSecond() instanceof TxRequest))
            return;

          final String leaderName = server.getHA().getLeaderName();

          messagesInTotal.incrementAndGet();
          messagesPerRestart.incrementAndGet();

          if (getServer(leaderName).isStarted() && messagesPerRestart.get() > getTxs() / (getServerCount() * 2) && restarts.get() < getServerCount() * 2) {
            LogManager.instance().log(this, Level.INFO, "TEST: Found online replicas %d", null, getServer(leaderName).getHA().getOnlineReplicas());

            if (getServer(leaderName).getHA().getOnlineReplicas() < getServerCount() - 1) {
              // NOT ALL THE SERVERS ARE UP, AVOID A QUORUM ERROR
              LogManager.instance()
                  .log(this, Level.FINE, "TEST: Skip restart of the Leader %s because no all replicas are online yet (messages=%d txs=%d) ...",null,  leaderName,
                      messagesInTotal.get(), getTxs());
              return;
            }

            if (semaphore.putIfAbsent(restarts.get(), true) != null)
              // ANOTHER REPLICA JUST DID IT
              return;

            LogManager.instance()
                .log(this, Level.INFO, "TEST: Stopping the Leader %s (messages=%d txs=%d restarts=%d) ...", null, leaderName, messagesInTotal.get(), getTxs(),
                    restarts.get());

            getServer(leaderName).stop();
            restarts.incrementAndGet();
            messagesPerRestart.set(0);

            executeAsynchronously(new Callable() {
              @Override
              public Object call() {
                getServer(leaderName).start();
                return null;
              }
            });
          }
        }
      }
    });
  }

  @Override
  protected int getTxs() {
    return 1000;
  }

  @Override
  protected int getVerticesPerTx() {
    return 10;
  }
}