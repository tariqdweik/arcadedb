/*
 * Copyright (c) - Arcade Data LTD (https://arcadedata.com)
 */

package com.arcadedb.server.ha;

import com.arcadedb.GlobalConfiguration;

public class ReplicationServerQuorumMajorityTest extends ReplicationServerTest {
  public ReplicationServerQuorumMajorityTest() {
    GlobalConfiguration.HA_QUORUM.setValue("Majority");
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