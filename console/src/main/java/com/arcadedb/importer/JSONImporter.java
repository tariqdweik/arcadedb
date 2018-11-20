/*
 * Copyright (c) 2018 - Arcade Analytics LTD (https://arcadeanalytics.com)
 */

package com.arcadedb.importer;

import com.arcadedb.database.Database;

import java.io.IOException;

public class JSONImporter implements ContentImporter {
  @Override
  public void load(SourceSchema sourceSchema, final Parser parser, final Database database, final ImporterContext context, final ImporterSettings settings) throws IOException {
  }

  @Override
  public SourceSchema analyze(final Parser parser, final ImporterSettings settings) {
    return new SourceSchema(this, parser.getSource(), null);
  }

  @Override
  public String getFormat() {
    return "JSON";
  }
}
