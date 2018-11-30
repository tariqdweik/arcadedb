/*
 * Copyright (c) 2018 - Arcade Analytics LTD (https://arcadeanalytics.com)
 */

package com.arcadedb.importer;

import com.arcadedb.database.DatabaseInternal;
import com.univocity.parsers.common.AbstractParser;

import java.io.IOException;
import java.io.InputStreamReader;

public class RDFImporter extends CSVImporter {
  private static final char[] STRING_CONTENT_SKIP = new char[] { '\'', '\'', '"', '"', '<', '>' };

  @Override
  public void load(final SourceSchema sourceSchema, AnalyzedEntity.ENTITY_TYPE entityType, final Parser parser, final DatabaseInternal database, final ImporterContext context,
      final ImporterSettings settings) throws ImportException {
    AbstractParser csvParser = createCSVParser(settings, ",");

    try (final InputStreamReader inputFileReader = new InputStreamReader(parser.getInputStream());) {
      csvParser.beginParsing(inputFileReader);

      if (!database.isTransactionActive())
        database.begin();

      String[] row;
      for (long line = 0; (row = csvParser.parseNext()) != null; ++line) {
        context.parsed.incrementAndGet();

        if (settings.skipEntries > 0 && line < settings.skipEntries)
          // SKIP IT
          continue;

        final String v1Id = getStringContent(row[0], STRING_CONTENT_SKIP);
        final String edgeLabel = getStringContent(row[1], STRING_CONTENT_SKIP);
        final String v2Id = getStringContent(row[2], STRING_CONTENT_SKIP);

        // CREATE AN EDGE
        database.newEdgeByKeys(settings.vertexTypeName, new String[] { settings.typeIdProperty }, new Object[] { v1Id },
            settings.vertexTypeName, new String[] { settings.typeIdProperty }, new Object[] { v2Id }, true, settings.edgeTypeName, true,
            "label", edgeLabel);

        context.createdEdges.incrementAndGet();
        context.parsed.incrementAndGet();

        if (context.parsed.get() % settings.commitEvery == 0) {
          database.commit();
          database.begin();
        }
      }

      database.commit();

    } catch (IOException e) {
      throw new ImportException("Error on importing CSV");
    }
  }

  @Override
  public String getFormat() {
    return "RDF";
  }
}
