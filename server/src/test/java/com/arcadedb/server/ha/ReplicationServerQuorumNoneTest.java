/*
 * Copyright (c) 2018 - Arcade Analytics LTD (https://arcadeanalytics.com)
 */

package com.arcadedb.server.ha;

import com.arcadedb.GlobalConfiguration;

public class ReplicationServerQuorumNoneTest extends ReplicationServerTest {
  public ReplicationServerQuorumNoneTest() {
    GlobalConfiguration.HA_QUORUM.setValue("NONE");
  }
}