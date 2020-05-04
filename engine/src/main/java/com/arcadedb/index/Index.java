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
 * Basic Index interface.
 */
public interface Index {
  interface BuildIndexCallback {
    void onDocumentIndexed(Document document, long totalIndexed);
  }

  /**
   * Retrieves the set of RIDs associated to a key.
   */
  IndexCursor get(Object[] keys);

  /**
   * Retrieves the set of RIDs associated to a key with a limit for the result.
   */
  IndexCursor get(Object[] keys, int limit);

  /**
   * Add multiple values for one key in the index.
   *
   * @param keys
   * @param rid  as an array of RIDs
   */
  void put(Object[] keys, RID[] rid);

  /**
   * Removes the keys from the index.
   *
   * @param keys
   */
  void remove(Object[] keys);

  /**
   * Removes an entry keys/record entry from the index.
   */
  void remove(Object[] keys, Identifiable rid);

  boolean compact() throws IOException, InterruptedException;

  boolean isCompacting();

  boolean scheduleCompaction();

  void setMetadata(String name, String[] propertyNames, int associatedBucketId);

  SchemaImpl.INDEX_TYPE getType();

  String getTypeName();

  String[] getPropertyNames();

  void close();

  void drop();

  String getName();

  Map<String, Long> getStats();

  LSMTreeIndexAbstract.NULL_STRATEGY getNullStrategy();

  void setNullStrategy(LSMTreeIndexAbstract.NULL_STRATEGY nullStrategy);

  int getFileId();

  boolean isUnique();

  PaginatedComponent getPaginatedComponent();

  int getAssociatedBucketId();

  boolean supportsOrderedIterations();

  boolean isAutomatic();

  long build(BuildIndexCallback callback);
}
