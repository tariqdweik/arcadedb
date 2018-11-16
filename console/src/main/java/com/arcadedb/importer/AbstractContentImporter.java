/*
 * Copyright (c) 2018 - Arcade Analytics LTD (https://arcadeanalytics.com)
 */

package com.arcadedb.importer;

import com.arcadedb.database.Database;
import com.arcadedb.database.MutableDocument;
import com.arcadedb.index.IndexCursor;

public abstract class AbstractContentImporter implements ContentImporter {
  private static final char[] STRING_CONTENT_SKIP = new char[] { '\'', '\'', '"', '"' };

  protected IndexCursor lookupRecord(final Database database, final String typeName, final String typeIdProperty, final Object id) {
    return database.lookupByKey(typeName, new String[] { typeIdProperty }, new Object[] { id });
  }

  protected MutableDocument createRecord(final Database database, final Importer.RECORD_TYPE recordType, final String typeName) {
    switch (recordType) {
    case DOCUMENT:
      return database.newDocument(typeName);
    case VERTEX:
      return database.newVertex(typeName);
    default:
      throw new IllegalArgumentException("recordType '" + recordType + "' not supported");
    }
  }

  protected String getStringContent(final String value) {
    return getStringContent(value, STRING_CONTENT_SKIP);
  }

  protected String getStringContent(final String value, final char[] chars) {
    if (value.length() > 1) {
      final char begin = value.charAt(0);

      for (int i = 0; i < chars.length - 1; i += 2) {
        if (begin == chars[i]) {
          final char end = value.charAt(value.length() - 1);
          if (end == chars[i + 1])
            return value.substring(1, value.length() - 1);
        }
      }
    }
    return value;
  }

}
