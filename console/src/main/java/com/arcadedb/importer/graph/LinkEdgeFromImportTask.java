/*
 * Copyright (c) - Arcade Data LTD (https://arcadedata.com)
 */

package com.arcadedb.importer.graph;

import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.Identifiable;
import com.arcadedb.database.async.DatabaseAsyncAbstractTask;
import com.arcadedb.database.async.DatabaseAsyncExecutor;
import com.arcadedb.graph.EdgeLinkedList;
import com.arcadedb.graph.EdgeSegment;
import com.arcadedb.graph.MutableVertex;
import com.arcadedb.graph.Vertex;
import com.arcadedb.utility.Pair;

import java.util.List;

/**
 * Asynchronous Task that links the destination vertex back to the edges/vertices.
 */
public class LinkEdgeFromImportTask extends DatabaseAsyncAbstractTask {
  private final Identifiable                           destinationVertex;
  private final List<Pair<Identifiable, Identifiable>> connections;
  private final EdgeLinkedCallback                     callback;

  public LinkEdgeFromImportTask(final Identifiable destinationVertex, final List<Pair<Identifiable, Identifiable>> connections,
      final EdgeLinkedCallback callback) {
    this.destinationVertex = destinationVertex;
    this.connections = connections;
    this.callback = callback;
  }

  public void execute(final DatabaseAsyncExecutor.AsyncThread async, final DatabaseInternal database) {

    final MutableVertex toVertexRecord = ((Vertex) destinationVertex.getRecord()).modify();

    final EdgeSegment inChunk = database.getGraphEngine().createInEdgeChunk(database, toVertexRecord);

    final EdgeLinkedList inLinkedList = new EdgeLinkedList(toVertexRecord, Vertex.DIRECTION.IN, inChunk);
    inLinkedList.addAll(connections);

    if (callback != null)
      callback.onLinked(connections.size());
  }

  @Override
  public String toString() {
    return "LinkEdgeFromImportTask(" + destinationVertex.getIdentity() + "<-" + connections.size() + ")";
  }
}
