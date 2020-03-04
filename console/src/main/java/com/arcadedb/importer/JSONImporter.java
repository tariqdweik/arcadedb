/*
 * Copyright (c) - Arcade Data LTD (https://arcadedata.com)
 */

package com.arcadedb.importer;

import com.arcadedb.database.DatabaseInternal;

import java.io.IOException;

public class JSONImporter implements ContentImporter {
  @Override
  public void load(SourceSchema sourceSchema, AnalyzedEntity.ENTITY_TYPE entityType, final Parser parser, final DatabaseInternal database,
      final ImporterContext context, final ImporterSettings settings) throws IOException {
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
