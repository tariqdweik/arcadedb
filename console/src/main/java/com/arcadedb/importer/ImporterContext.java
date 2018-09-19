/*
 * Copyright (c) 2018 - Arcade Analytics LTD (https://arcadeanalytics.com)
 */

package com.arcadedb.importer;

public class ImporterContext {
  long parsed;
  long startedOn;

  long createdVertices;
  long createdEdges;
  long createdDocuments;

  long lastLapOn;
  long lastParsed;
  long lastDocuments;
  long lastVertices;
  long lastEdges;
}