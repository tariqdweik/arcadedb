/*
 * Copyright (c) 2018 - Arcade Analytics LTD (https://arcadeanalytics.com)
 */

package com.arcadedb.server.ha;

import com.arcadedb.GlobalConfiguration;
import org.junit.jupiter.api.Test;

public class ReplicationServerWriteAgainstReplicaTest extends ReplicationServerTest {
  public ReplicationServerWriteAgainstReplicaTest() {
    GlobalConfiguration.HA_QUORUM.setValue("Majority");
  }

  @Override
  protected int getServerCount() {
    return 3;
  }

  @Test
  public void testReplication() {
    // WAIT THE LEADERSHIP HAS BEEN DETERMINED
    try {
      Thread.sleep(3000);
    } catch (InterruptedException e) {
      e.printStackTrace();
    }

    testReplication(1);
  }

  @Override
  protected int getTxs() {
    return 200;
  }

  @Override
  protected int getVerticesPerTx() {
    return 5000;
  }
}