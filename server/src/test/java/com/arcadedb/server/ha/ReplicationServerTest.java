/*
 * Copyright (c) 2018 - Arcade Analytics LTD (https://arcadeanalytics.com)
 */

package com.arcadedb.server.ha;

import com.arcadedb.server.BaseGraphServerTest;
import org.junit.jupiter.api.Test;

public class ReplicationServerTest extends BaseGraphServerTest {
  protected int getServers() {
    return 2;
  }

  @Test
  public void testReplication() {

  }
}