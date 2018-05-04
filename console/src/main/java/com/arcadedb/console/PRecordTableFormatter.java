/*
 *
 *  *  Copyright 2010-2016 OrientDB LTD (http://orientdb.com)
 *  *
 *  *  Licensed under the Apache License, Version 2.0 (the "License");
 *  *  you may not use this file except in compliance with the License.
 *  *  You may obtain a copy of the License at
 *  *
 *  *       http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  *  Unless required by applicable law or agreed to in writing, software
 *  *  distributed under the License is distributed on an "AS IS" BASIS,
 *  *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  *  See the License for the specific language governing permissions and
 *  *  limitations under the License.
 *  *
 *  * For more information: http://orientdb.com
 *
 */
package com.arcadedb.console;

import com.arcadedb.database.PDocument;
import com.arcadedb.database.PIdentifiable;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

public class PRecordTableFormatter extends PTableFormatter {

  public static class PTableRecordRow implements PTableRow {
    private final PDocument document;

    public PTableRecordRow(final PDocument document) {
      this.document = document;
    }

    @Override
    public Object getField(final String field) {
      if (field.equalsIgnoreCase("@rid"))
        return document.getIdentity();
      else if (field.equalsIgnoreCase("@type"))
        return document.getType();
      return document.get(field);
    }

    @Override
    public Set<String> getFields() {
      return document.getPropertyNames();
    }
  }

  public PRecordTableFormatter(final OTableOutput iConsole) {
    super(iConsole);
  }

  public void writeRecords(final List<PIdentifiable> records, final int limit) {
    final List<PTableRow> rows = new ArrayList<>();
    for (PIdentifiable id : records)
      rows.add(new PTableRecordRow((PDocument) id.getRecord()));

    super.writeRows(rows, limit);
  }
}
