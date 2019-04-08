/*
 * Copyright (c) 2019 - Arcade Analytics LTD (https://arcadeanalytics.com)
 */

package com.arcadedb.graph;

import com.arcadedb.database.Binary;
import com.arcadedb.database.Database;
import com.arcadedb.database.ImmutableDocument;

public class ImmutableEmbeddedDocument extends ImmutableDocument implements EmbeddedDocument {

  public ImmutableEmbeddedDocument(final Database database, final String typeName, final Binary buffer) {
    super(database, typeName, null, buffer);
  }

  @Override
  public byte getRecordType() {
    return EmbeddedDocument.RECORD_TYPE;
  }

  public MutableEmbeddedDocument modify() {
    checkForLazyLoading();
    buffer.rewind();
    return new MutableEmbeddedDocument(database, typeName, buffer.copy());
  }
}
