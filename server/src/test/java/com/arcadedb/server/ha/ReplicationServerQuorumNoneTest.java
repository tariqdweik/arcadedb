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

  @AfterEach
  @Override
  public void drop() {
    super.drop();
    GlobalConfiguration.HA_QUORUM.setValue("MAJORITY");
  }
}