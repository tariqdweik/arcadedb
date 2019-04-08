/*
 * Copyright (c) 2019 - Arcade Analytics LTD (https://arcadeanalytics.com)
 */

package com.arcadedb.graph;

import com.arcadedb.database.Document;

public interface EmbeddedDocument extends Document {
  byte RECORD_TYPE = 4;
}
