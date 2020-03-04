/*
 * Copyright (c) - Arcade Data LTD (https://arcadedata.com)
 */

package com.arcadedb.server.ha;

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.log.LogManager;
import com.arcadedb.server.ArcadeDBServer;
import com.arcadedb.server.TestCallback;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;

import java.io.IOException;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Level;

/**
 * Simulates a split brain on 5 nodes, by isolating nodes 4th and 5th in a separate network. After 10 seconds, allows the 2 networks to see
 * each other and hoping for a rejoin in only one network where the leaser is still the original one.
 */
public class HASplitBrainTest extends ReplicationServerTest {
  private final    Timer      timer     = new Timer();
  private final    AtomicLong messages  = new AtomicLong();
  private volatile boolean    split     = false;
  private volatile boolean    rejoining = false;

  public HASplitBrainTest() {
    GlobalConfiguration.HA_QUORUM.setValue("Majority");
  }

  @AfterEach
  @Override
  public void endTest() {
    super.endTest();
    GlobalConfiguration.HA_REPLICATION_QUEUE_SIZE.setValue(512);
  }

  @Override
  protected void onAfterTest() {
    timer.cancel();
    Assertions.assertEquals("ArcadeDB_0", getLeaderServer().getServerName());
  }

  @Override
  protected void onBeforeStarting(final ArcadeDBServer server) {
    if (server.getServerName().equals("ArcadeDB_3") || server.getServerName().equals("ArcadeDB_4"))
      server.registerTestEventListener(new TestCallback() {
        @Override
        public void onEvent(final TYPE type, final Object object, final ArcadeDBServer server) throws IOException {
          if (type == TYPE.NETWORK_CONNECTION && split) {
            final String connectTo = (String) object;

            if (connectTo.equals(getServer(0).getHA().getServerAddress()) || connectTo.equals(getServer(1).getHA().getServerAddress()) || connectTo
                .equals(getServer(2).getHA().getServerAddress()))
              if (!rejoining) {
                LogManager.instance().log(this, Level.INFO, "TEST: SIMULATING CONNECTION ERROR TO CONNECT TO THE LEADER FROM " + server);
                throw new IOException("Simulating an IO Exception on reconnecting from server '" + server.getServerName() + "' to " + connectTo);
              } else
                LogManager.instance().log(this, Level.INFO, "TEST: ALLOWED CONNECTION TO THE ADDRESS " + connectTo + "  FROM " + server);
          }
        }
      });

    if (server.getServerName().equals("ArcadeDB_4"))
      server.registerTestEventListener(new TestCallback() {
        @Override
        public void onEvent(final TestCallback.TYPE type, final Object object, final ArcadeDBServer server) {
          if (!split) {
            if (type == TYPE.REPLICA_MSG_RECEIVED) {
              messages.incrementAndGet();
              if (messages.get() > 10) {
                split = true;

                LogManager.instance().log(this, Level.INFO, "TEST: SHUTTING DOWN NETWORK CONNECTION BETWEEN SERVER 0 (THE LEADER) and SERVER 4TH and 5TH...");
                getServer(3).getHA().getLeader().closeChannel();
                getServer(0).getHA().getReplica("ArcadeDB_3").closeChannel();

                getServer(4).getHA().getLeader().closeChannel();
                getServer(0).getHA().getReplica("ArcadeDB_4").closeChannel();
                LogManager.instance().log(this, Level.INFO, "TEST: SHUTTING DOWN NETWORK CONNECTION COMPLETED");

                timer.schedule(new TimerTask() {
                  @Override
                  public void run() {
                    LogManager.instance().log(this, Level.INFO, "TEST: ALLOWING THE REJOINING OF SERVERS 4TH AND 5TH");
                    rejoining = true;
                  }
                }, 10000);
              }
            }
          }
        }
      });
  }

  @Override
  protected int getServerCount() {
    return 5;
  }

  @Override
  protected boolean isPrintingConfigurationAtEveryStep() {
    return true;
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