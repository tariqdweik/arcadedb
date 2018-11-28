/*
 * Copyright (c) 2018 - Arcade Analytics LTD (https://arcadeanalytics.com)
 */

package com.arcadedb.importer;

import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.index.CompressedAny2RIDIndex;

import java.io.IOException;

public class JSONImporter implements ContentImporter {
  @Override
  public void load(SourceSchema sourceSchema, AnalyzedEntity.ENTITY_TYPE entityType, final Parser parser, final DatabaseInternal database,
      final ImporterContext context, final ImporterSettings settings, final CompressedAny2RIDIndex inMemoryIndex) throws IOException {
  }

  @Override
  public SourceSchema analyze(AnalyzedEntity.ENTITY_TYPE entityType, final Parser parser, final ImporterSettings settings,
      AnalyzedSchema analyzedSchema) {
    return new SourceSchema(this, parser.getSource(), null);
  }

  @Override
  public String getFormat() {
    return "JSON";
  }
}
