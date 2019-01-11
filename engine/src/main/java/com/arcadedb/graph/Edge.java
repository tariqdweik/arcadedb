/*
 * Copyright (c) 2018 - Arcade Analytics LTD (https://arcadeanalytics.com)
 */

package com.arcadedb.graph;

import com.arcadedb.database.Document;
import com.arcadedb.database.RID;

public interface Edge extends Document {
  byte RECORD_TYPE = 2;

  MutableEdge modify();

  RID getOut();

  Vertex getOutVertex();

  RID getIn();

  Vertex getInVertex();

  Vertex getVertex(Vertex.DIRECTION iDirection);

  boolean isLightweight();
}
