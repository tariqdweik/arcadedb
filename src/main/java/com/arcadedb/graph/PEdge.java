package com.arcadedb.graph;

import com.arcadedb.database.PDocument;
import com.arcadedb.database.PRID;

public interface PEdge extends PDocument {
  byte RECORD_TYPE = 2;

  PRID getOut();

  PRID getIn();
}
