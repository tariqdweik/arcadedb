/*
 * Copyright (c) 2018 - Arcade Analytics LTD (https://arcadeanalytics.com)
 */

package com.arcadedb.importer;

import com.arcadedb.database.Binary;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.graph.GraphEngine;
import com.arcadedb.graph.VertexInternal;
import com.arcadedb.index.CompressedAny2RIDIndex;
import com.arcadedb.index.CompressedRID2RIDsIndex;

import java.util.ArrayList;
import java.util.List;

public class GraphImporter {
  private static final Object[] NO_PARAMS = new Object[] {};
  public static final  int      _32MB     = 32 * 1024 * 1024;

  private final DatabaseInternal             database;
  private final GraphImporterThreadContext[] threadContexts;

  public class GraphImporterThreadContext {
    Binary                  vertexIndexThreadBuffer;
    CompressedRID2RIDsIndex incomingConnectionsIndex;

    Long                                  lastSourceKey    = null;
    VertexInternal                        lastSourceVertex = null;
    List<GraphEngine.CreateEdgeOperation> connections      = new ArrayList<>();
    int                                   importedEdges    = 0;

    public GraphImporterThreadContext(final int expectedVertices) {
      incomingConnectionsIndex = new CompressedRID2RIDsIndex(database, expectedVertices);
    }
  }

  public GraphImporter(final DatabaseInternal database, final int expectedVertices) {
    this.database = database;

    final int parallel = database.async().getParallelLevel();
    threadContexts = new GraphImporterThreadContext[parallel];
    for (int i = 0; i < parallel; ++i)
      threadContexts[i] = new GraphImporterThreadContext(expectedVertices);
  }

  public void createInternalBuffers(final CompressedAny2RIDIndex verticesIndex) {
    for (int i = 0; i < threadContexts.length; ++i)
      threadContexts[i].vertexIndexThreadBuffer = verticesIndex.getInternalBuffer().slice();
  }

  public void close() {
    for (int i = 0; i < threadContexts.length; ++i)
      threadContexts[i] = null;
  }

  //TODO SUPPORT NOT ONLY LONGS AS VERTICES KEYS
  public void createEdge(final long sourceVertexKey, final String edgeTypeName, final long destinationVertexKey,
      final Object[] edgeProperties, final ImporterContext context, final ImporterSettings settings) {
    final int slot = database.async().getSlot((int) sourceVertexKey);

    database.async().scheduleTask(slot,
        new CreateEdgeFromImportTask(threadContexts[slot], edgeTypeName, sourceVertexKey, destinationVertexKey, edgeProperties, context,
            settings), true);
  }
}
