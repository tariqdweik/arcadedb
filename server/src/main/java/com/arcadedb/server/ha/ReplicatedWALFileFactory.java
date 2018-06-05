/*
 * Copyright (c) 2018 - Arcade Analytics LTD (https://arcadeanalytics.com)
 */

package com.arcadedb.server.ha;

import com.arcadedb.engine.WALFile;
import com.arcadedb.engine.WALFileFactory;

import java.io.FileNotFoundException;

public class ReplicatedWALFileFactory implements WALFileFactory {
  public WALFile newInstance(final String filePath) throws FileNotFoundException {
    return new ReplicatedWALFile(filePath);
  }
}
