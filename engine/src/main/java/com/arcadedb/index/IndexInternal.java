/*
 * Copyright (c) - Arcade Data LTD (https://arcadedata.com)
 */

package com.arcadedb.index;

import com.arcadedb.database.Document;
import com.arcadedb.database.Identifiable;
import com.arcadedb.database.RID;
import com.arcadedb.engine.PaginatedComponent;
import com.arcadedb.index.lsm.LSMTreeIndexAbstract;
import com.arcadedb.schema.SchemaImpl;

import java.io.IOException;
import java.util.Map;

/**
 * Internal Index interface.
 */
public interface IndexInternal extends Index {
  boolean compact() throws IOException, InterruptedException;

  void setMetadata(String name, String[] propertyNames, int associatedBucketId);

  void close();

  void drop();

  String getName();

  Map<String, Long> getStats();

  int getFileId();

  PaginatedComponent getPaginatedComponent();
}
