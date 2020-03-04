/*
 * Copyright (c) - Arcade Data LTD (https://arcadedata.com)
 */

package com.arcadedb.index;

import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.engine.PaginatedFile;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class IndexFactory {
  private final Map<String, IndexFactoryHandler> map = new HashMap<>();

  public void register(final String type, final IndexFactoryHandler handler) {
    map.put(type, handler);
  }

  public Index createIndex(final String indexType, final DatabaseInternal database, final String indexName, final boolean unique, final String filePath,
      final PaginatedFile.MODE mode, final byte[] keyTypes, final int pageSize, final Index.BuildIndexCallback callback) throws IOException {
    final IndexFactoryHandler handler = map.get(indexType);
    if (handler == null)
      throw new IllegalArgumentException("Cannot create index of type '" + indexType + "'");

    return handler.create(database, indexName, unique, filePath, mode, keyTypes, pageSize, callback);
  }
}
