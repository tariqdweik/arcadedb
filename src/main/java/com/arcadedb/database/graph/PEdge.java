package com.arcadedb.database.graph;

import com.arcadedb.database.PIdentifiable;
import com.arcadedb.database.PRecord;

public interface PEdge extends PRecord {
  byte RECORD_TYPE = 2;

  PIdentifiable getOut();

  PIdentifiable getIn();
}
