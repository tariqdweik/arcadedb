/*
 * Copyright (c) 2018 - Arcade Analytics LTD (https://arcadeanalytics.com)
 */

package com.arcadedb.server.ha;

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.server.ArcadeDBServer;
import com.arcadedb.server.TestCallback;
import com.arcadedb.utility.LogManager;

import java.util.concurrent.atomic.AtomicInteger;

public class ReplicationServerQuorumMajority1ServerOutTest extends ReplicationServerTest {
  private final AtomicInteger messages = new AtomicInteger();

  public ReplicationServerQuorumMajority1ServerOutTest() {
    GlobalConfiguration.HA_QUORUM.setValue("Majority");
  }

  @Override
  protected void onBeforeStarting(final ArcadeDBServer server) {
    if (server.getServerName().equals("ArcadeDB_2"))
      server.registerTestEventListener(new TestCallback() {
        @Override
        public void onEvent(final TYPE type, final Object object, final ArcadeDBServer server) {
          if (type == TYPE.REPLICA_MSG_RECEIVED) {
            if (messages.incrementAndGet() > 100) {
              LogManager.instance().info(this, "TEST: Stopping Replica 2...");
              getServer(2).stop();
            }
          }
        }
      });
  }

  protected int[] getServerToCheck() {
    final int[] result = new int[getServerCount() - 1];
    for (int i = 0; i < result.length; ++i)
      result[i] = i;
    return result;
  }

  @Override
  protected int getTxs() {
    return 300;
  }

}