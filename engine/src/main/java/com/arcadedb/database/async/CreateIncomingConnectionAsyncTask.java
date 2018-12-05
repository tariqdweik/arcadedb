/*
 * Copyright (c) 2018 - Arcade Analytics LTD (https://arcadeanalytics.com)
 */

package com.arcadedb.database.async;

import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.Identifiable;
import com.arcadedb.utility.Pair;

import java.util.List;

/**
 * Asynchronous Task that creates the relationship between the destinationVertex and the sourceVertices as incoming.
 */
public class CreateIncomingConnectionAsyncTask extends DatabaseAsyncAbstractTask {
  private final Identifiable                           destinationVertex;
  private final List<Pair<Identifiable, Identifiable>> connections;
  private final NewEdgeBackLinkingCallback             newEdgeBackLinkingCallback;

  public CreateIncomingConnectionAsyncTask(final Identifiable destinationVertex, final List<Pair<Identifiable, Identifiable>> connections,
      final NewEdgeBackLinkingCallback newEdgeBackLinkingCallback) {
    this.destinationVertex = destinationVertex;
    this.connections = connections;
    this.newEdgeBackLinkingCallback = newEdgeBackLinkingCallback;
  }

  public void execute(final DatabaseAsyncExecutor.AsyncThread async, final DatabaseInternal database) {
    database.getGraphEngine().connectIncomingEdges(database, destinationVertex, connections, newEdgeBackLinkingCallback);
  }

  @Override
  public String toString() {
    return "CreateIncomingConnectionAsyncTask(" + destinationVertex + "<-" + connections.size() + ")";
  }
}
