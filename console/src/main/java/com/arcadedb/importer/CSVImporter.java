/*
 * Copyright (c) 2018 - Arcade Analytics LTD (https://arcadeanalytics.com)
 */

package com.arcadedb.importer;

import com.arcadedb.database.Database;
import com.arcadedb.database.MutableDocument;
import com.univocity.parsers.common.AbstractParser;
import com.univocity.parsers.csv.CsvParser;
import com.univocity.parsers.csv.CsvParserSettings;
import com.univocity.parsers.tsv.TsvParser;
import com.univocity.parsers.tsv.TsvParserSettings;

import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Map;

public class CSVImporter extends AbstractContentImporter {

  @Override
  public void load(final Parser parser, final Database database, final ImporterContext context, final ImporterSettings settings) throws ImportException {
    String delimiter = ",";

    for (Map.Entry<String, String> opt : settings.options.entrySet()) {
      if ("delimiter".equals(opt.getKey()))
        delimiter = opt.getValue();
    }

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

      String[] row;
      while ((row = csvParser.parseNext()) != null) {

        if (!database.isTransactionActive())
          database.begin();

        onParsedLine(database, context, settings, row);
      }
    } catch (IOException e) {
      throw new ImportException("Error on importing CSV");
    }
  }

  protected void onParsedLine(final Database database, final ImporterContext context, final ImporterSettings settings, final String[] row) {
    final MutableDocument record = createRecord(database, settings.recordType,
        settings.recordType == Importer.RECORD_TYPE.VERTEX ? settings.vertexTypeName : settings.documentTypeName);
    database.asynch().createRecord(record);
    ++context.parsed;

    if (settings.recordType == Importer.RECORD_TYPE.VERTEX)
      ++context.createdVertices;
    else
      ++context.createdDocuments;
  }

  @Override
  public SourceSchema analyze(Parser parser, final ImporterSettings settings) {
    return new SourceSchema(this, parser.getSource(), null);
  }

  @Override
  public String getFormat() {
    return "CSV";
  }
}
