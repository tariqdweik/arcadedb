/*
 * Copyright (c) 2018 - Arcade Analytics LTD (https://arcadeanalytics.com)
 */

package com.arcadedb.server.ha;

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.database.Database;
import com.arcadedb.exception.TransactionException;
import com.arcadedb.server.ArcadeDBServer;
import com.arcadedb.server.TestCallback;
import com.arcadedb.utility.LogManager;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.concurrent.atomic.AtomicInteger;

public class ReplicationServerQuorumMajority2ServersOutTest extends ReplicationServerTest {
  private final AtomicInteger messages = new AtomicInteger();

  public ReplicationServerQuorumMajority2ServersOutTest() {
    GlobalConfiguration.HA_QUORUM.setValue("Majority");
  }

  @Override
  protected void onBeforeStarting(final ArcadeDBServer server) {
    if (server.getServerName().equals("ArcadeDB_1"))
      server.registerTestEventListener(new TestCallback() {
        @Override
        public void onEvent(final TYPE type, final Object object, final ArcadeDBServer server) {
          if (type == TYPE.REPLICA_MSG_RECEIVED) {
            if (messages.incrementAndGet() > 100) {
              LogManager.instance().info(this, "TEST: Replica 1 is offline");
              getServer(1).stop();
            }
          }
        }
      });

    if (server.getServerName().equals("ArcadeDB_2"))
      server.registerTestEventListener(new TestCallback() {
        @Override
        public void onEvent(final TYPE type, final Object object, final ArcadeDBServer server) {
          if (type == TYPE.REPLICA_MSG_RECEIVED) {
            if (messages.incrementAndGet() > 200) {
              LogManager.instance().info(this, "TEST: Replica 2 is offline");
              getServer(2).stop();
            }
          }
        }
      });
  }

  @Test
  public void testReplication() {
    try {
      super.testReplication();
      Assertions.fail("Replication is supposed to fail without enough online servers");
    } catch (TransactionException e) {
      Throwable sub = e.getCause();
      Assertions.assertTrue(sub.toString().contains("Quorum") && sub.toString().contains("not reached"));
    }
  }

  protected int[] getServerToCheck() {
    final int[] result = new int[getServerCount()];
    for (int i = 0; i < result.length; ++i)
      result[i] = i;
    return result;
  }

  protected void checkEntriesOnServer(final int s) {
    final Database db = getServer(s).getDatabase(getDatabaseName());
    db.begin();
    try {
      Assertions.assertTrue(1 + getTxs() * getVerticesPerTx() > db.countType(VERTEX1_TYPE_NAME, true), "Check for vertex count for server" + s);

    } catch (Exception e) {
      e.printStackTrace();
      Assertions.fail("Error on checking on server" + s);
    } finally {
      db.close();
    }
  }

  @Override
  protected boolean isPrintingConfigurationAtEveryStep() {
    return false;
  }

  @Override
  protected int getTxs() {
    return 500;
  }

}