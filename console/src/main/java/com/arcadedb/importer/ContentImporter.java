/*
 * Copyright (c) - Arcade Data LTD (https://arcadedata.com)
 */

package com.arcadedb.importer;

import com.arcadedb.database.DatabaseInternal;

import java.io.IOException;

public interface ContentImporter {
  void load(SourceSchema sourceSchema, AnalyzedEntity.ENTITY_TYPE entityType, Parser parser, DatabaseInternal database,
      ImporterContext context, ImporterSettings settings) throws IOException;

  SourceSchema analyze(AnalyzedEntity.ENTITY_TYPE entityType, Parser parser, ImporterSettings settings, AnalyzedSchema analyzedSchema)
      throws IOException;

  String getFormat();
}
