/*
 * Copyright (c) 2019 - Arcade Data LTD (https://arcadedata.com)
 */

package com.arcadedb.graph;

import com.arcadedb.database.Binary;
import com.arcadedb.database.Database;
import com.arcadedb.database.ImmutableDocument;
import com.arcadedb.database.Record;

public class ImmutableEmbeddedDocument extends ImmutableDocument implements EmbeddedDocument {

  public ImmutableEmbeddedDocument(final Database database, final String typeName, final Binary buffer) {
    super(database, typeName, null, buffer);
  }

  @Override
  public byte getRecordType() {
    return EmbeddedDocument.RECORD_TYPE;
  }

  public MutableEmbeddedDocument modify() {
    final Record recordInCache = database.getTransaction().getRecordFromCache(rid);
    if (recordInCache != null && recordInCache != this && recordInCache instanceof MutableEmbeddedDocument)
      return (MutableEmbeddedDocument) recordInCache;

    checkForLazyLoading();
    buffer.rewind();
    return new MutableEmbeddedDocument(database, typeName, buffer.copy());
  }
}
