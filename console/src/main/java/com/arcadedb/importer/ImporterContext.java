/*
 * Copyright (c) 2018 - Arcade Analytics LTD (https://arcadeanalytics.com)
 */

package com.arcadedb.importer;

import java.util.concurrent.atomic.AtomicLong;

public class ImporterContext {
  long startedOn;

  AtomicLong parsed           = new AtomicLong();
  AtomicLong createdVertices  = new AtomicLong();
  AtomicLong createdEdges     = new AtomicLong();
  AtomicLong createdDocuments = new AtomicLong();

  AtomicLong skippedEdges     = new AtomicLong();

  long lastLapOn;
  long lastParsed;
  long lastDocuments;
  long lastVertices;
  long lastEdges;
}