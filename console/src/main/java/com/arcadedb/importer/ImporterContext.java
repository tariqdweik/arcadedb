/*
 * Copyright (c) 2018 - Arcade Analytics LTD (https://arcadeanalytics.com)
 */

package com.arcadedb.importer;

import com.arcadedb.index.CompressedAny2RIDIndex;

import java.util.concurrent.atomic.AtomicLong;

public class ImporterContext {
  CompressedAny2RIDIndex<Object> verticesIndex;
  long                           startedOn;

  AtomicLong parsed = new AtomicLong();

  AtomicLong createdDocuments = new AtomicLong();
  AtomicLong createdVertices  = new AtomicLong();
  AtomicLong createdEdges     = new AtomicLong();
  AtomicLong linkedEdges      = new AtomicLong();

  AtomicLong skippedEdges = new AtomicLong();

  long lastLapOn;
  long lastParsed;
  long lastDocuments;
  long lastVertices;
  long lastEdges;
  long lastLinkedEdges;
}