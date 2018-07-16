/*
 * Copyright (c) 2018 - Arcade Analytics LTD (https://arcadeanalytics.com)
 */

package com.arcadedb.server.ha;

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.database.Database;
import com.arcadedb.network.binary.QuorumNotReachedException;
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
              LogManager.instance().info(this, "TEST: Stopping Replica 1...");
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
              LogManager.instance().info(this, "TEST: Stopping Replica 2...");
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
    } catch (QuorumNotReachedException e) {
      // CATCH IT
    }
  }

  protected int[] getServerToCheck() {
    return new int[]{};
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
  protected int getTxs() {
    return 500;
  }

}