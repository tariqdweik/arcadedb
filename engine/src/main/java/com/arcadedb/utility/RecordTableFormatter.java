/*
 * Copyright (c) 2018 - Arcade Analytics LTD (https://arcadeanalytics.com)
 */
package com.arcadedb.utility;

import com.arcadedb.database.Document;
import com.arcadedb.sql.executor.Result;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

public class RecordTableFormatter extends TableFormatter {

  public static class PTableRecordRow implements PTableRow {
    private final Result result;

    public PTableRecordRow(final Result result) {
      this.result = result;
    }

    @Override
    public Object getField(final String field) {
      if (field.equalsIgnoreCase("@rid")) {
        if (result.getIdentity().isPresent())
          return result.getIdentity().get();
      } else if (field.equalsIgnoreCase("@type")) {
        if (result.getRecord().isPresent())
          return ((Document) result.getRecord().get()).getType();
      }
      return result.getProperty(field);
    }

    @Override
    public Set<String> getFields() {
      return result.getPropertyNames();
    }
  }

  public RecordTableFormatter(final OTableOutput iConsole) {
    super(iConsole);
  }

  public void writeRecords(final List<Result> records, final int limit) {
    final List<PTableRow> rows = new ArrayList<>();
    for (Result record : records)
      rows.add(new PTableRecordRow(record));

    super.writeRows(rows, limit);
  }
}
