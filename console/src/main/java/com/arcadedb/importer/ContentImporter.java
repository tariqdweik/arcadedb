/*
 * Copyright (c) 2018 - Arcade Analytics LTD (https://arcadeanalytics.com)
 */

package com.arcadedb.importer;

import com.arcadedb.database.Database;

import java.io.IOException;

public interface ContentImporter {
  void load(Parser parser, Database database, ImporterContext context, ImporterSettings settings) throws IOException;

  SourceSchema analyze(Parser parser, ImporterSettings settings) throws IOException;

  String getFormat();
}
