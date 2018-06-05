/*
 * Copyright (c) 2018 - Arcade Analytics LTD (https://arcadeanalytics.com)
 */

package com.arcadedb.engine;

import java.io.FileNotFoundException;

public interface WALFileFactory {
  WALFile newInstance(final String filePath) throws FileNotFoundException;
}
