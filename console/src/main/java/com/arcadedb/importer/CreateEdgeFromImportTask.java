/*
 * Copyright (c) 2018 - Arcade Analytics LTD (https://arcadeanalytics.com)
 */

package com.arcadedb.importer;

import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.Identifiable;
import com.arcadedb.database.RID;
import com.arcadedb.database.async.DatabaseAsyncAbstractTask;
import com.arcadedb.database.async.DatabaseAsyncExecutor;
import com.arcadedb.graph.*;
import com.arcadedb.index.CompressedRID2RIDsIndex;
import com.arcadedb.log.LogManager;
import com.arcadedb.utility.FileUtils;
import com.arcadedb.utility.Pair;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;
import java.util.logging.Level;

/**
 * Asynchronous Task that creates the relationship between the sourceVertex and the destinationVertex as outgoing.
 */
public class CreateEdgeFromImportTask extends DatabaseAsyncAbstractTask {
  private final GraphImporter.GraphImporterThreadContext threadContext;
  private final String                                   edgeTypeName;
  private final long                                     sourceVertexKey;
  private final long                                     destinationVertexKey;
  private final Object[]                                 params;
  private final ImporterContext                          context;
  private final ImporterSettings                         settings;

  public CreateEdgeFromImportTask(final GraphImporter.GraphImporterThreadContext threadContext, final String edgeTypeName,
      final long sourceVertexKey, final long destinationVertexKey, final Object[] edgeProperties, final ImporterContext context,
      final ImporterSettings settings) {
    this.threadContext = threadContext;
    this.edgeTypeName = edgeTypeName;
    this.sourceVertexKey = sourceVertexKey;
    this.destinationVertexKey = destinationVertexKey;
    this.params = edgeProperties;
    this.context = context;
    this.settings = settings;
  }

  public void execute(final DatabaseAsyncExecutor.AsyncThread async, final DatabaseInternal database) {

//    LogManager.instance().log(this, Level.INFO, "Using context %s from theadId=%d", null, threadContext, Thread.currentThread().getId());

    // TODO: LOAD FROM INDEX
    final RID destinationVertexRID = context.verticesIndex.get(threadContext.vertexIndexThreadBuffer, destinationVertexKey);
    if (destinationVertexRID == null) {
      // SKIP IT
      context.skippedEdges.incrementAndGet();
      return;
    }

    if (threadContext.lastSourceKey == null || !threadContext.lastSourceKey.equals(sourceVertexKey)) {
      createEdgesInBatch(database, threadContext.incomingConnectionsIndex, context, settings, threadContext.connections);
      threadContext.connections = new ArrayList<>();

      // TODO: LOAD FROM INDEX
      final RID sourceVertexRID = context.verticesIndex.get(threadContext.vertexIndexThreadBuffer, sourceVertexKey);
      if (sourceVertexRID == null) {
        // SKIP IT
        context.skippedEdges.incrementAndGet();
        return;
      }

      threadContext.lastSourceKey = sourceVertexKey;
      threadContext.lastSourceVertex = (VertexInternal) sourceVertexRID.getVertex(true);
    }

    threadContext.connections.add(new GraphEngine.CreateEdgeOperation(edgeTypeName, destinationVertexRID, params));

    ++threadContext.importedEdges;

    if (threadContext.incomingConnectionsIndex.getChunkSize() >= settings.maxRAMIncomingEdges) {
      LogManager.instance()
          .log(this, Level.INFO, "Creation of back connections, reached %s size (max=%s), flushing %d connections (slots=%d thread=%d)...",
              null, FileUtils.getSizeAsString(threadContext.incomingConnectionsIndex.getChunkSize()),
              FileUtils.getSizeAsString(settings.maxRAMIncomingEdges), threadContext.incomingConnectionsIndex.size(),
              threadContext.incomingConnectionsIndex.getTotalUsedSlots(), Thread.currentThread().getId());

      createIncomingEdgesInBatch(database, threadContext.incomingConnectionsIndex);

      // CREATE A NEW CHUNK BEFORE CONTINUING
      threadContext.incomingConnectionsIndex = new CompressedRID2RIDsIndex(database, threadContext.incomingConnectionsIndex.getKeys());

      LogManager.instance().log(this, Level.INFO, "Creation done, reset index buffer and continue", null);
    }

    if (threadContext.importedEdges % settings.commitEvery == 0) {
//      LogManager.instance().log(this, Level.INFO, "Committing batch of outgoing edges (chunkSize=%s max=%s entries=%d slots=%d)...", null,
//          FileUtils.getSizeAsString(threadContext.incomingConnectionsIndex.getChunkSize()),
//          FileUtils.getSizeAsString(settings.maxRAMIncomingEdges), threadContext.incomingConnectionsIndex.size(),
//          threadContext.incomingConnectionsIndex.getTotalUsedSlots());

      createEdgesInBatch(database, threadContext.incomingConnectionsIndex, context, settings, threadContext.connections);
      threadContext.connections = new ArrayList<>();
    }
  }

  private void createEdgesInBatch(final DatabaseInternal database, final CompressedRID2RIDsIndex edgeIndex, final ImporterContext context,
      final ImporterSettings settings, final List<GraphEngine.CreateEdgeOperation> connections) {
    if (!connections.isEmpty()) {
      // CREATE EDGES ALL TOGETHER FOR THE PREVIOUS BATCH
      if (threadContext.lastSourceVertex.getOutEdgesHeadChunk() == null)
        // RELOAD IT
        threadContext.lastSourceVertex = (VertexInternal) threadContext.lastSourceVertex.getIdentity().getVertex();

      final List<Edge> newEdges = database.getGraphEngine().newEdges(database, threadContext.lastSourceVertex, connections, false);

      context.createdEdges.addAndGet(newEdges.size());

      for (Edge e : newEdges)
        edgeIndex.put(e.getIn(), e.getIdentity(), threadContext.lastSourceVertex.getIdentity());

      connections.clear();
    }
  }

  private void createIncomingEdgesInBatch(final DatabaseInternal database, final CompressedRID2RIDsIndex index) {
    Vertex lastVertex = null;
    List<Pair<Identifiable, Identifiable>> connections = new ArrayList<>();

    long totalVertices = 0;
    long totalEdges = 0;
    int minEdges = Integer.MAX_VALUE;
    int maxEdges = -1;

    for (final CompressedRID2RIDsIndex.EntryIterator it = index.entryIterator(); it.hasNext(); it.moveNext()) {
      final Vertex destinationVertex = it.getKeyRID().getVertex(true);

      if (!connections.isEmpty() && !destinationVertex.equals(lastVertex)) {
        ++totalVertices;

        if (connections.size() < minEdges)
          minEdges = connections.size();
        if (connections.size() > maxEdges)
          maxEdges = connections.size();

        connectIncomingEdges(database, lastVertex, connections);

        connections = new ArrayList<>();
      }

      lastVertex = destinationVertex;

      connections.add(new Pair<>(it.getEdgeRID(), it.getVertexRID()));

      ++totalEdges;
    }

    if (lastVertex != null)
      connectIncomingEdges(database, lastVertex, connections);

    LogManager.instance()
        .log(this, Level.INFO, "Created %d back connections from %d vertices (min=%d max=%d avg=%d)", null, totalEdges, totalVertices,
            minEdges, maxEdges, totalVertices > 0 ? totalEdges / totalVertices : 0);
  }

  public void connectIncomingEdges(final DatabaseInternal database, final Identifiable toVertex,
      final List<Pair<Identifiable, Identifiable>> connections) {

    VertexInternal toVertexRecord = (VertexInternal) toVertex.getRecord();

    final AtomicReference<VertexInternal> toVertexRef = new AtomicReference<>(toVertexRecord);
    final EdgeChunk inChunk = database.getGraphEngine().createInEdgeChunk(database, toVertexRef);
    toVertexRecord = toVertexRef.get();

    final EdgeLinkedList inLinkedList = new EdgeLinkedList(toVertexRecord, Vertex.DIRECTION.IN, inChunk);
    inLinkedList.addAll(connections);

    context.linkedEdges.addAndGet(connections.size());
  }

  @Override
  public String toString() {
    return "CreateEdgeFromImportTask(" + sourceVertexKey + "->" + destinationVertexKey + ")";
  }
}
