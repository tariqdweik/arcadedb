/*
 * Copyright (c) - Arcade Data LTD (https://arcadedata.com)
 */

package com.arcadedb.engine;

import java.io.FileNotFoundException;

public class WALFileFactoryEmbedded implements WALFileFactory {
  public WALFile newInstance(final String filePath) throws FileNotFoundException {
    return new WALFile(filePath);
  }
}
