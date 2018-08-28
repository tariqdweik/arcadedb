/*
 * Copyright (c) 2018 - Arcade Analytics LTD (https://arcadeanalytics.com)
 */

package com.arcadedb.importer.csv;

import com.arcadedb.database.MutableDocument;
import com.arcadedb.importer.AbstractImporter;
import com.univocity.parsers.common.AbstractParser;
import com.univocity.parsers.csv.CsvParser;
import com.univocity.parsers.csv.CsvParserSettings;
import com.univocity.parsers.tsv.TsvParser;
import com.univocity.parsers.tsv.TsvParserSettings;

public class CSVImporter extends AbstractImporter {
  protected CsvParserSettings csvParserSettings;
  protected TsvParserSettings tsvParserSettings;
  protected AbstractParser    parser;

  public CSVImporter(final String[] args) {
    super(args);
    if ("\\t".equals(delimiter)) {
      tsvParserSettings = new TsvParserSettings();
      parser = new TsvParser(tsvParserSettings);
    } else {
      csvParserSettings = new CsvParserSettings();
      parser = new CsvParser(csvParserSettings);
      csvParserSettings.getFormat().setDelimiter(delimiter.charAt(0));
    }
  }

  public static void main(final String[] args) {
    new CSVImporter(args).load();
  }

  protected void load() {
    openDatabase();
    try {
      parser.beginParsing(openInputFile());

      startImporting();

      String[] row;
      while ((row = parser.parseNext()) != null) {

        onParsedLine(row);

        ++parsed;
      }

      database.asynch().waitCompletion();

      stopImporting();

    } catch (Exception e) {
      e.printStackTrace();
    } finally {
      closeDatabase();
      closeInputFile();
    }
  }

  protected void onParsedLine(final String[] row) {
    final MutableDocument record = createRecord(recordType, vertexTypeName);
    database.asynch().createRecord(record);
  }

  @Override
  protected long getInputFilePosition() {
    return parser.getContext().currentChar();
  }

  @Override
  protected void parseParameter(final String name, final String value) {
    if ("-recordType".equals(name))
      recordType = RECORD_TYPE.valueOf(value.toUpperCase());
    else
      super.parseParameter(name, value);
  }
}
