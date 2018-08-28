/*
 * Copyright (c) 2018 - Arcade Analytics LTD (https://arcadeanalytics.com)
 */

package com.arcadedb.importer.rdf;

import com.arcadedb.database.async.NewEdgeCallback;
import com.arcadedb.graph.Edge;
import com.arcadedb.importer.csv.CSVImporter;

import java.util.HashMap;
import java.util.Map;

public class RDFImporter extends CSVImporter {
  private static final char[]              STRING_CONTENT_SKIP = new char[] { '\'', '\'', '"', '"', '<', '>' };
  private              Map<String, String> mapping;
  private              long                lastCompaction      = System.currentTimeMillis();

  public RDFImporter(final String[] args) {
    super(args);
    recordType = RECORD_TYPE.VERTEX;
    parallel = 1;
  }

  public static void main(final String[] args) {
    new RDFImporter(args).load();
  }

  @Override
  protected void onParsedLine(final String[] row) {
    final String v1Id = getStringContent(row[0], STRING_CONTENT_SKIP);
    final String edgeLabel = getStringContent(row[1], STRING_CONTENT_SKIP);
    final String v2Id = getStringContent(row[2], STRING_CONTENT_SKIP);

    beginTxIfNeeded();

    // CREATE AN EDGE
    database.asynch().newEdgeByKeys(vertexTypeName, new String[] { typeIdProperty }, new Object[] { v1Id }, vertexTypeName, new String[] { typeIdProperty },
        new Object[] { v2Id }, true, edgeTypeName, true, new NewEdgeCallback() {
          @Override
          public void call(final Edge newEdge, final boolean createdSourceVertex, final boolean createdDestinationVertex) {
            if (createdSourceVertex)
              ++createdVertices;
            if (createdDestinationVertex)
              ++createdVertices;
          }
        }, "label", edgeLabel);

    ++createdEdges;

    if (parsed % commitEvery == 0) {
      database.commit();
      database.begin();
    }
  }

  @Override
  protected void parseParameter(final String name, final String value) {
    if ("-map".equals(name)) {
      final String[] values = getStringContent(value, new char[] { '\'', '"' }).split(":");
      if (mapping == null)
        mapping = new HashMap<>();
      mapping.put(values[0], values[1]);
    } else
      super.parseParameter(name, value);
  }
}
