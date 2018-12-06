/*
 * Copyright (c) 2018 - Arcade Analytics LTD (https://arcadeanalytics.com)
 */

package com.arcadedb.importer;

import com.arcadedb.database.*;
import com.arcadedb.database.async.NewRecordCallback;
import com.arcadedb.graph.GraphEngine;
import com.arcadedb.graph.MutableVertex;
import com.arcadedb.graph.Vertex;
import com.arcadedb.graph.VertexInternal;
import com.arcadedb.index.CompressedAny2RIDIndex;
import com.arcadedb.index.CompressedRID2RIDsIndex;
import com.arcadedb.schema.Type;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

public class GraphImporter {
  private final CompressedAny2RIDIndex       verticesIndex;
  private final DatabaseInternal             database;
  private final GraphImporterThreadContext[] threadContexts;

  enum STATUS {IMPORTING_VERTEX, IMPORTING_EDGE, CLOSED}

  private STATUS status = STATUS.IMPORTING_VERTEX;

  public class GraphImporterThreadContext {
    Binary                  vertexIndexThreadBuffer;
    CompressedRID2RIDsIndex incomingConnectionsIndexThread;

    Long                                  lastSourceKey    = null;
    VertexInternal                        lastSourceVertex = null;
    List<GraphEngine.CreateEdgeOperation> connections      = new ArrayList<>();
    int                                   importedEdges    = 0;

    public GraphImporterThreadContext(final int expectedVertices) {
      incomingConnectionsIndexThread = new CompressedRID2RIDsIndex(database, expectedVertices);
    }
  }

  public GraphImporter(final DatabaseInternal database, final int expectedVertices) {
    this.database = database;

    this.verticesIndex = new CompressedAny2RIDIndex(database, Type.LONG, expectedVertices);

    final int parallel = database.async().getParallelLevel();
    threadContexts = new GraphImporterThreadContext[parallel];
    for (int i = 0; i < parallel; ++i)
      threadContexts[i] = new GraphImporterThreadContext(expectedVertices);
  }

  public void close(final ImporterContext context) {
    database.commit();
    database.begin();

    for (int i = 0; i < threadContexts.length; ++i) {
      CreateEdgeFromImportTask.createIncomingEdgesInBatch(database, threadContexts[i].incomingConnectionsIndexThread, context);
      threadContexts[i] = null;
    }
    database.commit();

    status = STATUS.CLOSED;
  }

  public RID getVertex(final Binary vertexIndexThreadBuffer, final long vertexId) {
    return verticesIndex.get(vertexIndexThreadBuffer, vertexId);
  }

  public RID getVertex(final long vertexId) {
    return verticesIndex.get(vertexId);
  }

  public void createVertex(final String vertexTypeName, final long vertexId, final Object[] vertexProperties) {

    final Vertex sourceVertex;
    RID sourceVertexRID = verticesIndex.get(vertexId);
    if (sourceVertexRID == null) {
      // CREATE THE VERTEX
      sourceVertex = database.newVertex(vertexTypeName);
      ((MutableVertex) sourceVertex).set(vertexProperties);

      database.async().createRecord((MutableDocument) sourceVertex, new NewRecordCallback() {
        @Override
        public void call(final Record newDocument) {
          final AtomicReference<VertexInternal> v = new AtomicReference<>((VertexInternal) sourceVertex);
          // PRE-CREATE OUT/IN CHUNKS TO SPEEDUP EDGE CREATION
          final DatabaseInternal db = (DatabaseInternal) database;
          db.getGraphEngine().createOutEdgeChunk(db, v);
          db.getGraphEngine().createInEdgeChunk(db, v);

          verticesIndex.put(vertexId, newDocument.getIdentity());
        }
      });
    }
  }

  //TODO SUPPORT NOT ONLY LONGS AS VERTICES KEYS
  public void createEdge(final long sourceVertexKey, final String edgeTypeName, final long destinationVertexKey,
      final Object[] edgeProperties, final ImporterContext context, final ImporterSettings settings) {
    final int slot = database.async().getSlot((int) sourceVertexKey);

    database.async().scheduleTask(slot,
        new CreateEdgeFromImportTask(threadContexts[slot], edgeTypeName, sourceVertexKey, destinationVertexKey, edgeProperties, context,
            settings), true);
  }

  public void startImportingEdges() {
    if (status != STATUS.IMPORTING_VERTEX)
      throw new IllegalStateException("Cannot import edges on current status " + status);

    status = STATUS.IMPORTING_EDGE;

    for (int i = 0; i < threadContexts.length; ++i)
      threadContexts[i].vertexIndexThreadBuffer = verticesIndex.getInternalBuffer().slice();
  }

  public CompressedAny2RIDIndex<Object> getVerticesIndex() {
    return verticesIndex;
  }

}
