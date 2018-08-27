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

import java.io.IOException;

public class CSVImporter extends AbstractImporter {
  private final CsvParserSettings csvParserSettings = new CsvParserSettings();
  private final TsvParserSettings tsvParserSettings = new TsvParserSettings();
  private       AbstractParser    parser;

  public CSVImporter(final String[] args) {
    super(args);
    if ("\\t".equals(delimiter))
      parser = new TsvParser(tsvParserSettings);
    else {
      parser = new CsvParser(csvParserSettings);
      csvParserSettings.getFormat().setDelimiter(delimiter.charAt(0));
    }
  }

  public static void main(final String[] args) {
    new CSVImporter(args).load();
  }

  private void load() {
    openDatabase();
    try {
      parser.beginParsing(openInputFile());

      //csvParser.getRecordMetadata().headers();

      startImporting();

      String[] row;
      while ((row = parser.parseNext()) != null) {

        final MutableDocument record = createRecord();

        database.asynch().createRecord(record);
        ++parsed;
      }

      database.asynch().waitCompletion();

      stopImporting();

    } catch (IOException e) {
      e.printStackTrace();
    } finally {
      closeDatabase();
      closeInputFile();
    }
  }

  @Override
  protected long getInputFilePosition() {
    return parser.getContext().currentChar();
  }
}
