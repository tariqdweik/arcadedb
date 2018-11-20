/*
 * Copyright (c) 2018 - Arcade Analytics LTD (https://arcadeanalytics.com)
 */

package com.arcadedb.importer;

import com.arcadedb.database.Database;

public class RDFImporter extends CSVImporter {
  private static final char[] STRING_CONTENT_SKIP = new char[] { '\'', '\'', '"', '"', '<', '>' };

  @Override
  protected void onParsedLine(SourceSchema sourceSchema, final Database database, final ImporterContext context, final ImporterSettings settings,
      final String[] row) {
    final String v1Id = getStringContent(row[0], STRING_CONTENT_SKIP);
    final String edgeLabel = getStringContent(row[1], STRING_CONTENT_SKIP);
    final String v2Id = getStringContent(row[2], STRING_CONTENT_SKIP);

    // CREATE AN EDGE
    database.newEdgeByKeys(settings.vertexTypeName, new String[] { settings.typeIdProperty }, new Object[] { v1Id }, settings.vertexTypeName,
        new String[] { settings.typeIdProperty }, new Object[] { v2Id }, true, settings.edgeTypeName, true, "label", edgeLabel);

    context.createdEdges.incrementAndGet();
    context.parsed.incrementAndGet();

    if (context.parsed.get() % settings.commitEvery == 0) {
      database.commit();
      database.begin();
    }
  }

  @Override
  public String getFormat() {
    return "RDF";
  }
}
