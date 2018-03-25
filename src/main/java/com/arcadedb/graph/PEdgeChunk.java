package com.arcadedb.graph;

import com.arcadedb.database.PBinary;
import com.arcadedb.database.PRID;
import com.arcadedb.database.PRecord;

import java.util.concurrent.atomic.AtomicInteger;

public interface PEdgeChunk extends PRecord {
  byte RECORD_TYPE = 3;

  boolean add(PRID edgeRID, PRID vertexRID);

  boolean containsEdge(PRID rid);

  boolean containsVertex(PRID rid);

  PEdgeChunk getNext();

  void setNext(PEdgeChunk next);

  PBinary getContent();

  int getUsed();

  PRID getEdge(AtomicInteger currentPosition);

  PRID getVertex(AtomicInteger currentPosition);

  int getRecordSize();
}
