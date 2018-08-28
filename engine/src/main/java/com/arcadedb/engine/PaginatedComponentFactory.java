/*
 * Copyright (c) 2018 - Arcade Analytics LTD (https://arcadeanalytics.com)
 */

package com.arcadedb.engine;

import com.arcadedb.database.Database;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class PaginatedComponentFactory {
  private final Map<String, PaginatedComponentFactoryHandler> map = new HashMap<>();
  private final Database                                      database;

  public interface PaginatedComponentFactoryHandler {
    PaginatedComponent createOnLoad(final Database database, final String name, final String filePath, final int id, final PaginatedFile.MODE mode,
        final int pageSize) throws IOException;
  }

  public PaginatedComponentFactory(final Database database) {
    this.database = database;
  }

  public void registerComponent(final String fileExt, final PaginatedComponentFactoryHandler handler) {
    map.put(fileExt, handler);
  }

  public PaginatedComponent createComponent(final PaginatedFile file, final PaginatedFile.MODE mode) throws IOException {
    final String fileName = file.getComponentName();
    final int fileId = file.getFileId();
    final String fileExt = file.getFileExtension();
    final int pageSize = file.getPageSize();

    final PaginatedComponentFactoryHandler handler = map.get(fileExt);
    if (handler != null)
      return handler.createOnLoad(database, fileName, file.getFilePath(), fileId, mode, pageSize);

    return null;
  }
}
