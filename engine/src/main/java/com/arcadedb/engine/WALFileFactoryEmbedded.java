/*
 * Copyright (c) - Arcade Analytics LTD (https://arcadeanalytics.com)
 */

package com.arcadedb.engine;

import java.io.FileNotFoundException;

public class WALFileFactoryEmbedded implements WALFileFactory {
  public WALFile newInstance(final String filePath) throws FileNotFoundException {
    return new WALFile(filePath);
  }
}
