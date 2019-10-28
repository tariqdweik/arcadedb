/*
 * Copyright (c) - Arcade Analytics LTD (https://arcadeanalytics.com)
 */
package com.arcadedb.server.ha.message;

import com.arcadedb.database.Binary;

public abstract class HAAbstractCommand implements HACommand {
  @Override
  public void toStream(final Binary stream) {
  }

  @Override
  public void fromStream(final Binary stream) {
  }
}
