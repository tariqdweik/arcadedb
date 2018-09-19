/*
 * Copyright (c) 2018 - Arcade Analytics LTD (https://arcadeanalytics.com)
 */

package com.arcadedb.importer;

import com.arcadedb.database.Database;
import com.arcadedb.database.async.NewEdgeCallback;
import com.arcadedb.graph.Edge;

public class RDFImporter extends CSVImporter {
  private static final char[] STRING_CONTENT_SKIP = new char[] { '\'', '\'', '"', '"', '<', '>' };

  @Override
  protected void onParsedLine(final Database database, final ImporterContext context, final ImporterSettings settings, final String[] row) {
    final String v1Id = getStringContent(row[0], STRING_CONTENT_SKIP);
    final String edgeLabel = getStringContent(row[1], STRING_CONTENT_SKIP);
    final String v2Id = getStringContent(row[2], STRING_CONTENT_SKIP);

    // CREATE AN EDGE
    database.asynch().newEdgeByKeys(settings.vertexTypeName, new String[] { settings.typeIdProperty }, new Object[] { v1Id }, settings.vertexTypeName,
        new String[] { settings.typeIdProperty }, new Object[] { v2Id }, true, settings.edgeTypeName, true, new NewEdgeCallback() {
          @Override
          public void call(final Edge newEdge, final boolean createdSourceVertex, final boolean createdDestinationVertex) {
            if (createdSourceVertex)
              ++context.createdVertices;
            if (createdDestinationVertex)
              ++context.createdVertices;
          }
        }, "label", edgeLabel);

    ++context.createdEdges;

    if (++context.parsed % settings.commitEvery == 0) {
      database.commit();
      database.begin();
    }
  }

  @Override
  public String getFormat() {
    return "RDF";
  }
}
