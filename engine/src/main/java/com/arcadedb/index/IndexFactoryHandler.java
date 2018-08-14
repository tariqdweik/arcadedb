/*
 * Copyright (c) 2018 - Arcade Analytics LTD (https://arcadeanalytics.com)
 */

package com.arcadedb.index;

import com.arcadedb.database.Database;
import com.arcadedb.engine.PaginatedFile;

import java.io.IOException;

public interface IndexFactoryHandler {
  Index create(final Database database, final String name, final boolean unique, final String filePath, final PaginatedFile.MODE mode,
      final String[] propertyNames, final byte[] keyTypes, final int pageSize) throws IOException;
}
