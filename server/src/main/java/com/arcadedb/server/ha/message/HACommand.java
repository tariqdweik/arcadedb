/*
 * Copyright (c) - Arcade Data LTD (https://arcadedata.com)
 */
package com.arcadedb.server.ha.message;

import com.arcadedb.database.Binary;
import com.arcadedb.server.ha.HAServer;

public interface HACommand {
  HACommand execute(HAServer server, String remoteServerName, long messageNumber);

  void toStream(Binary stream);

  void fromStream(Binary stream);
}
