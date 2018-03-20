package com.arcadedb.database;

public interface PEdge extends PRecord {
  byte RECORD_TYPE = 2;

  PIdentifiable getOut();

  PIdentifiable getIn();
}
