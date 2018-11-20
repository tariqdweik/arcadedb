/*
 * Copyright (c) 2018 - Arcade Analytics LTD (https://arcadeanalytics.com)
 */

package com.arcadedb.importer;

import com.arcadedb.database.Database;
import com.arcadedb.database.MutableDocument;
import com.arcadedb.graph.MutableVertex;
import com.univocity.parsers.common.AbstractParser;
import com.univocity.parsers.csv.CsvParser;
import com.univocity.parsers.csv.CsvParserSettings;
import com.univocity.parsers.tsv.TsvParser;
import com.univocity.parsers.tsv.TsvParserSettings;

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;

public class CSVImporter extends AbstractContentImporter {

  @Override
  public void load(final SourceSchema sourceSchema, final Parser parser, final Database database, final ImporterContext context,
      final ImporterSettings settings) throws ImportException {
    String delimiter = ",";

    if (settings.options.containsKey("delimiter"))
      delimiter = settings.options.get("delimiter");

    if (settings.skipEntries == null)
      // BY DEFAULT SKIP THE FIRST LINE AS HEADER
      settings.skipEntries = 1l;

    CsvParserSettings csvParserSettings;
    TsvParserSettings tsvParserSettings;
    AbstractParser csvParser;

    if ("\t".equals(delimiter)) {
      tsvParserSettings = new TsvParserSettings();
      csvParser = new TsvParser(tsvParserSettings);
    } else {
      csvParserSettings = new CsvParserSettings();
      csvParser = new CsvParser(csvParserSettings);
      csvParserSettings.getFormat().setDelimiter(delimiter.charAt(0));
    }

    try (final InputStreamReader inputFileReader = new InputStreamReader(parser.getInputStream());) {

      csvParser.beginParsing(inputFileReader);

      if (!database.isTransactionActive())
        database.begin();

      String[] row;
      for (long line = 0; (row = csvParser.parseNext()) != null; ++line) {
        if (settings.skipEntries > 0 && line < settings.skipEntries)
          // SKIP IT
          continue;

        onParsedLine(sourceSchema, database, context, settings, row);

        if (settings.commitEvery > 0 && line > 0 && line % settings.commitEvery == 0) {
          database.commit();
          database.begin();
        }
      }

      database.commit();

    } catch (IOException e) {
      throw new ImportException("Error on importing CSV");
    }
  }

  protected void onParsedLine(final SourceSchema sourceSchema, final Database database, final ImporterContext context, final ImporterSettings settings,
      final String[] row) {

    switch (settings.recordType) {
    case DOCUMENT:
      MutableDocument document = database.newDocument(settings.typeName);
      database.async().createRecord(document);
      context.parsed.incrementAndGet();
      context.createdDocuments.incrementAndGet();
      break;

    case VERTEX:
      MutableVertex vertex = database.newVertex(settings.typeName);
      database.async().createRecord(vertex);
      context.parsed.incrementAndGet();
      context.createdVertices.incrementAndGet();
      break;

    case EDGE:
      final AnalyzedProperty from = sourceSchema.getSchema().getProperty(settings.edgeTypeName, settings.edgeFromField);
      final AnalyzedProperty to = sourceSchema.getSchema().getProperty(settings.edgeTypeName, settings.edgeToField);

      database.newEdgeByKeys(settings.vertexTypeName, new String[] { settings.typeIdProperty }, new Object[] { row[from.getIndex()] }, settings.vertexTypeName,
          new String[] { settings.typeIdProperty }, new Object[] { row[to.getIndex()] }, true, settings.edgeTypeName, true);
      context.parsed.incrementAndGet();
      context.createdEdges.incrementAndGet();
      break;

    default:
      throw new IllegalArgumentException("recordType '" + settings.recordType + "' not supported");
    }
  }

  @Override
  public SourceSchema analyze(final Parser parser, final ImporterSettings settings) throws IOException {
    final AnalyzedSchema schema = new AnalyzedSchema(100);

    parser.reset();

    String delimiter = ",";

    if (settings.options.containsKey("delimiter"))
      delimiter = settings.options.get("delimiter");

    CsvParserSettings csvParserSettings;
    TsvParserSettings tsvParserSettings;
    AbstractParser csvParser;

    if ("\t".equals(delimiter)) {
      tsvParserSettings = new TsvParserSettings();
      csvParser = new TsvParser(tsvParserSettings);
    } else {
      csvParserSettings = new CsvParserSettings();
      csvParser = new CsvParser(csvParserSettings);
      csvParserSettings.setDelimiterDetectionEnabled(false);
      csvParserSettings.detectFormatAutomatically(delimiter.charAt(0));
      csvParserSettings.getFormat().setDelimiter(delimiter.charAt(0));
    }

    final List<String> fieldNames = new ArrayList<>();

    try (final InputStreamReader inputFileReader = new InputStreamReader(parser.getInputStream());) {
      csvParser.beginParsing(inputFileReader);

      String[] row;
      for (long line = 0; (row = csvParser.parseNext()) != null; ++line) {
        if (settings.analysisLimitBytes > 0 && csvParser.getContext().currentChar() > settings.analysisLimitBytes)
          break;

        if (settings.analysisLimitEntries > 0 && line > settings.analysisLimitEntries)
          break;

        if (line == 0) {
          // HEADER
          for (String cell : row)
            fieldNames.add(cell);
        } else
          // DATA LINE
          for (int i = 0; i < row.length; ++i)
            schema.setProperty(settings.typeName, fieldNames.get(i), row[i]);

      }

    } catch (EOFException e) {
      // REACHED THE LIMIT
    } catch (IOException e) {
      throw new ImportException("Error on importing CSV");
    }

    // END OF PARSING. THIS DETERMINES THE TYPE
    schema.endParsing();

    return new SourceSchema(this, parser.getSource(), schema);
  }

  @Override
  public String getFormat() {
    return "CSV";
  }
}
