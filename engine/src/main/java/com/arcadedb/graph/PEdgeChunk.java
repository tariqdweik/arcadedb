package com.arcadedb.graph;

import com.arcadedb.database.PBinary;
import com.arcadedb.database.PRID;
import com.arcadedb.database.PRecord;

import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

public interface PEdgeChunk extends PRecord {
  byte RECORD_TYPE = 3;

  boolean add(PRID edgeRID, PRID vertexRID);

  boolean containsEdge(PRID edgeRID);

  boolean containsVertex(PRID vertexRID);

  int removeEdge(PRID edgeRID);

  int removeVertex(PRID vertexRID);

  PEdgeChunk getNext();

  void setNext(PEdgeChunk next);

  PBinary getContent();

  int getUsed();

  PRID getEdge(AtomicInteger currentPosition);

  PRID getVertex(AtomicInteger currentPosition);

  int getRecordSize();

  long count(Set<Integer> fileIds);
}
