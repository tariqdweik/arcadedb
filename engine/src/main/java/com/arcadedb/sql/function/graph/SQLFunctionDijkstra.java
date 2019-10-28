/*
 * Copyright (c) - Arcade Analytics LTD (https://arcadeanalytics.com)
 */
package com.arcadedb.sql.function.graph;

import com.arcadedb.database.Identifiable;
import com.arcadedb.graph.Vertex;
import com.arcadedb.sql.executor.CommandContext;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;

/**
 * Dijkstra's algorithm describes how to find the cheapest path from one node to another node in a directed weighted graph.
 * <p>
 * The first parameter is source record. The second parameter is destination record. The third parameter is a name of property that
 * represents 'weight'.
 * <p>
 * If property is not defined in edge or is null, distance between vertexes are 0.
 *
 * @author Luca Garulli (l.garulli--(at)--orientdb.com)
 */
public class SQLFunctionDijkstra extends SQLFunctionPathFinder {
  public static final String NAME = "dijkstra";

  private String paramWeightFieldName;

  public SQLFunctionDijkstra() {
    super(NAME, 3, 4);
  }

  public LinkedList<Vertex> execute( final Object iThis, final Identifiable iCurrentRecord,
      final Object iCurrentResult, final Object[] iParams, final CommandContext iContext) {
    return new SQLFunctionAstar().execute(this, iCurrentRecord, iCurrentResult, toAStarParams(iParams), iContext);
  }

  private Object[] toAStarParams(Object[] iParams) {
    Object[] result = new Object[4];
    result[0] = iParams[0];
    result[1] = iParams[1];
    result[2] = iParams[2];
    Map<String, Object> options = new HashMap<String, Object>();
    options.put("emptyIfMaxDepth", true);
    if (iParams.length > 3) {
      options.put("direction", iParams[3]);
    }
    result[3] = options;
    return result;
  }

  private LinkedList<Vertex> internalExecute(final CommandContext iContext) {
    return super.execute(iContext);
  }

  public String getSyntax() {
    return "dijkstra(<sourceVertex>, <destinationVertex>, <weightEdgeFieldName>, [<direction>])";
  }

  protected float getDistance(final Vertex node, final Vertex target) {
    return -1;//not used anymore
  }

  @Override
  protected boolean isVariableEdgeWeight() {
    return true;
  }
}
