/*
 * Copyright (c) 2018 - Arcade Analytics LTD (https://arcadeanalytics.com)
 */
package com.arcadedb.server.ha.message;

import com.arcadedb.database.Binary;
import com.arcadedb.server.ha.HAServer;

public interface HACommand {
  HACommand execute(HAServer server);

  void toStream(Binary stream);

  void fromStream(Binary stream);
}
