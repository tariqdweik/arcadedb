/*
 * Copyright (c) 2019 - Arcade Data LTD (https://arcadedata.com)
 */

package com.arcadedb.graph;

import com.arcadedb.database.Binary;
import com.arcadedb.database.Database;
import com.arcadedb.database.Document;
import com.arcadedb.database.MutableDocument;

public class MutableEmbeddedDocument extends MutableDocument implements EmbeddedDocument {
  /**
   * Creation constructor.
   */
  public MutableEmbeddedDocument(final Database db, final String typeName) {
    super(db, typeName, null);
  }

  /**
   * Copy constructor from ImmutableVertex.modify().
   */
  public MutableEmbeddedDocument(final Database db, final String typeName, final Binary buffer) {
    super(db, typeName, null, buffer);
  }

  @Override
  public MutableEmbeddedDocument save() {
    throw new UnsupportedOperationException("Embedded document cannot be saved");
  }

  @Override
  public MutableEmbeddedDocument save(final String bucketName) {
    throw new UnsupportedOperationException("Embedded document cannot be saved");
  }

  @Override
  public void reload() {
    throw new UnsupportedOperationException("Embedded document cannot be reloaded");

  }

  @Override
  public MutableEmbeddedDocument modify() {
    return this;
  }

  @Override
  public byte getRecordType() {
    return EmbeddedDocument.RECORD_TYPE;
  }
}
