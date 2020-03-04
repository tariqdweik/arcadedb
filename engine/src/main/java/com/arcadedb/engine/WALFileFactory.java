/*
 * Copyright (c) - Arcade Data LTD (https://arcadedata.com)
 */

package com.arcadedb.engine;

import java.io.FileNotFoundException;

public interface WALFileFactory {
  WALFile newInstance(final String filePath) throws FileNotFoundException;
}
