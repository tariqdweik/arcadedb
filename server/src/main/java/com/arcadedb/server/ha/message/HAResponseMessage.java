/*
 * Copyright (c) 2018 - Arcade Analytics LTD (https://arcadeanalytics.com)
 */
package com.arcadedb.server.ha.message;

import com.arcadedb.server.ha.HAServer;

public interface HAResponseMessage<T extends HARequestMessage> extends HAMessage {
  void build(HAServer server, T request);
}
