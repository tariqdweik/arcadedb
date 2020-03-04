/*
 * Copyright (c) 2019 - Arcade Data LTD (https://arcadedata.com)
 */

package com.arcadedb.graph;

import com.arcadedb.database.Document;

public interface EmbeddedDocument extends Document {
  byte RECORD_TYPE = 4;
}
