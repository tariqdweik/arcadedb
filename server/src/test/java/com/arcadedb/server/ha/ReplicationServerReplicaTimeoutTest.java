/*
 * Copyright (c) 2018 - Arcade Analytics LTD (https://arcadeanalytics.com)
 */

package com.arcadedb.server.ha;

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.server.ArcadeDBServer;
import com.arcadedb.server.TestCallback;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;

import java.util.concurrent.atomic.AtomicLong;

public class ReplicationServerReplicaTimeoutTest extends ReplicationServerTest {
  private final AtomicLong totalMessages = new AtomicLong();

  public ReplicationServerReplicaTimeoutTest() {
    GlobalConfiguration.HA_QUORUM.setValue("MAJORITY");
    GlobalConfiguration.TEST.setValue(true);
    GlobalConfiguration.HA_REPLICATION_QUEUE_SIZE.setValue(10);
  }

  @AfterEach
  @Override
  public void drop() {
    super.drop();
    GlobalConfiguration.TEST.setValue(false);
    GlobalConfiguration.HA_REPLICATION_QUEUE_SIZE.setValue(512);
  }

  @Override
  protected int[] getServerToCheck() {
    return new int[] { 0, 1 };
  }

  @Override
  protected void onBeforeStarting(final ArcadeDBServer server) {
    if (server.getServerName().equals("ArcadeDB_2"))
      server.registerTestEventListener(new TestCallback() {
        @Override
        public void onEvent(final TYPE type, final Object object, final ArcadeDBServer server) {
          // SLOW DOWN A SERVER AFTER 5TH MESSAGE
          if (type == TYPE.REPLICA_MSG_RECEIVED) {
            if (totalMessages.incrementAndGet() > 5) {
              try {
                Thread.sleep(1000);
              } catch (InterruptedException e) {
                e.printStackTrace();
              }
            }
          }
        }
      });

    if (server.getServerName().equals("ArcadeDB_0"))
      server.registerTestEventListener(new TestCallback() {
        @Override
        public void onEvent(final TYPE type, final Object object, final ArcadeDBServer server) {
          // SLOW DOWN A SERVER
          if (type == TYPE.SERVER_SHUTTING_DOWN) {
            Assertions.assertEquals(1, server.getHA().getOnlineReplicas());
          }
        }
      });
  }
}