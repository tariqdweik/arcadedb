/*
 * Copyright (c) - Arcade Data LTD (https://arcadedata.com)
 */

package com.arcadedb.importer;

import com.arcadedb.importer.graph.GraphImporter;

import java.util.concurrent.atomic.AtomicLong;

public class ImporterContext {
  public GraphImporter graphImporter;
  public long          startedOn;

  public final AtomicLong parsed           = new AtomicLong();
  public final AtomicLong createdDocuments = new AtomicLong();
  public final AtomicLong createdVertices  = new AtomicLong();
  public final AtomicLong createdEdges     = new AtomicLong();
  public final AtomicLong linkedEdges      = new AtomicLong();
  public final AtomicLong skippedEdges     = new AtomicLong();

  public long lastLapOn;
  public long lastParsed;
  public long lastDocuments;
  public long lastVertices;
  public long lastEdges;
  public long lastLinkedEdges;
}