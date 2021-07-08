/*
 * Copyright 2021 Arcade Data Ltd
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
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
