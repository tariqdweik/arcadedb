/*
 * Copyright (c) 2018 - Arcade Analytics LTD (https://arcadeanalytics.com)
 */

package com.arcadedb.server.ha;

import com.arcadedb.GlobalConfiguration;
import org.junit.jupiter.api.AfterEach;

public class ReplicationServerQuorumAllTest extends ReplicationServerTest {
  public ReplicationServerQuorumAllTest() {
    GlobalConfiguration.HA_QUORUM.setValue("ALL");
  }

  @AfterEach
  @Override
  public void endTest() {
    super.endTest();
    GlobalConfiguration.HA_QUORUM.setValue("MAJORITY");
  }
}