/*
 * Copyright (c) - Arcade Data LTD (https://arcadedata.com)
 */

package com.arcadedb.graph;

import com.arcadedb.database.Binary;
import com.arcadedb.database.RID;
import com.arcadedb.database.Record;

import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

public interface EdgeSegment extends Record {
  byte RECORD_TYPE = 3;

  boolean add(RID edgeRID, RID vertexRID);

  boolean containsEdge(RID edgeRID);

  boolean containsVertex(RID vertexRID);

  int removeEdge(RID edgeRID);

  int removeVertex(RID vertexRID);

  EdgeSegment getNext();

  void setNext(EdgeSegment next);

  Binary getContent();

  int getUsed();

  RID getRID(AtomicInteger currentPosition);

  int getRecordSize();

  long count(Set<Integer> fileIds);
}
