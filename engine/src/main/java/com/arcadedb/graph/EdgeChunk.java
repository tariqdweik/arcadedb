/*
 * Copyright (c) 2018 - Arcade Analytics LTD (https://arcadeanalytics.com)
 */

package com.arcadedb.graph;

import com.arcadedb.database.Binary;
import com.arcadedb.database.RID;
import com.arcadedb.database.Record;

import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

public interface EdgeChunk extends Record {
  byte RECORD_TYPE = 3;

  boolean add(RID edgeRID, RID vertexRID);

  boolean containsEdge(RID edgeRID);

  boolean containsVertex(RID vertexRID);

  int removeEdge(RID edgeRID);

  int removeVertex(RID vertexRID);

  EdgeChunk getNext();

  void setNext(EdgeChunk next);

  Binary getContent();

  int getUsed();

  RID getEdge(AtomicInteger currentPosition);

  RID getVertex(AtomicInteger currentPosition);

  int getRecordSize();

  long count(Set<Integer> fileIds);
}
