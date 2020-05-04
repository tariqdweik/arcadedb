/*
 * Copyright (c) - Arcade Data LTD (https://arcadedata.com)
 */

package com.arcadedb.index;

import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.engine.PaginatedFile;
import com.arcadedb.index.lsm.LSMTreeIndexAbstract;

import java.io.IOException;

public interface IndexFactoryHandler {
  Index create(final DatabaseInternal database, final String name, final boolean unique, final String filePath, final PaginatedFile.MODE mode,
      final byte[] keyTypes, final int pageSize, LSMTreeIndexAbstract.NULL_STRATEGY nullStrategy, Index.BuildIndexCallback callback) throws IOException;
}
