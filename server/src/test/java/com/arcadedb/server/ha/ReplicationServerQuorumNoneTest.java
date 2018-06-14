/*
 * Copyright (c) 2018 - Arcade Analytics LTD (https://arcadeanalytics.com)
 */

package com.arcadedb.server.ha;

import com.arcadedb.GlobalConfiguration;
import org.junit.jupiter.api.AfterEach;

public class ReplicationServerQuorumNoneTest extends ReplicationServerTest {
  public ReplicationServerQuorumNoneTest() {
    GlobalConfiguration.HA_QUORUM.setValue("NONE");
  }

  @Override
  protected boolean isPrintingConfigurationAtEveryStep() {
    return false;
  }

  @Override
  protected int getTxs() {
    return 200;
  }

  @Override
  protected int getVerticesPerTx() {
    return 5000;
  }

  @AfterEach
  @Override
  public void drop() {
    try {
      Thread.sleep(5000);
    } catch (InterruptedException e) {
      e.printStackTrace();
    }

    super.drop();
    GlobalConfiguration.HA_QUORUM.setValue("MAJORITY");
  }
}