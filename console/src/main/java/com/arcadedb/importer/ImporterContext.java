/*
 * Copyright (c) 2018 - Arcade Analytics LTD (https://arcadeanalytics.com)
 */

package com.arcadedb.importer;

import java.util.concurrent.atomic.AtomicLong;

public class ImporterContext {
  GraphImporter graphImporter;
  long          startedOn;

  final AtomicLong parsed           = new AtomicLong();
  final AtomicLong createdDocuments = new AtomicLong();
  final AtomicLong createdVertices  = new AtomicLong();
  final AtomicLong createdEdges     = new AtomicLong();
  final AtomicLong linkedEdges      = new AtomicLong();
  final AtomicLong skippedEdges     = new AtomicLong();

  long lastLapOn;
  long lastParsed;
  long lastDocuments;
  long lastVertices;
  long lastEdges;
  long lastLinkedEdges;
}