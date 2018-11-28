/*
 * Copyright (c) 2018 - Arcade Analytics LTD (https://arcadeanalytics.com)
 */

package com.arcadedb.importer;

import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.index.CompressedAny2RIDIndex;

import java.io.IOException;

public interface ContentImporter {
  void load(SourceSchema sourceSchema, AnalyzedEntity.ENTITY_TYPE entityType, Parser parser, DatabaseInternal database,
      ImporterContext context, ImporterSettings settings, CompressedAny2RIDIndex<Long> inMemoryIndex) throws IOException;

  SourceSchema analyze(AnalyzedEntity.ENTITY_TYPE entityType, Parser parser, ImporterSettings settings, AnalyzedSchema analyzedSchema)
      throws IOException;

  String getFormat();
}
