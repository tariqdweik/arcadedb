package com.arcadedb.graph;

import com.arcadedb.database.PRID;
import com.arcadedb.database.PRecord;

public interface PEdge extends PRecord {
  byte RECORD_TYPE = 2;

  PRID getOut();

  PRID getIn();
}
