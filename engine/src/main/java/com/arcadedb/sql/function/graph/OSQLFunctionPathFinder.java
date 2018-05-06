/*
 *
 *  *  Copyright 2010-2016 OrientDB LTD (http://orientdb.com)
 *  *
 *  *  Licensed under the Apache License, Version 2.0 (the "License");
 *  *  you may not use this file except in compliance with the License.
 *  *  You may obtain a copy of the License at
 *  *
 *  *       http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  *  Unless required by applicable law or agreed to in writing, software
 *  *  distributed under the License is distributed on an "AS IS" BASIS,
 *  *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  *  See the License for the specific language governing permissions and
 *  *  limitations under the License.
 *  *
 *  * For more information: http://orientdb.com
 *
 */
package com.arcadedb.sql.function.graph;

import com.arcadedb.database.PRID;
import com.arcadedb.graph.PVertex;
import com.arcadedb.sql.executor.OCommandContext;
import com.arcadedb.sql.function.math.OSQLFunctionMathAbstract;

import java.util.*;

/**
 * Abstract class to find paths between nodes.
 *
 * @author Luca Garulli (l.garulli--(at)--orientdb.com)
 */
public abstract class OSQLFunctionPathFinder extends OSQLFunctionMathAbstract {
  protected Set<PVertex>       unSettledNodes;
  protected Map<PRID, PVertex> predecessors;
  protected Map<PRID, Float>   distance;

  protected PVertex           paramSourceVertex;
  protected PVertex           paramDestinationVertex;
  protected PVertex.DIRECTION paramDirection = PVertex.DIRECTION.OUT;
  protected OCommandContext   context;

  protected static final float MIN = 0f;

  public OSQLFunctionPathFinder(final String iName, final int iMinParams, final int iMaxParams) {
    super(iName, iMinParams, iMaxParams);
  }

  protected LinkedList<PVertex> execute(final OCommandContext iContext) {
    context = iContext;
    unSettledNodes = new HashSet<PVertex>();
    distance = new HashMap<PRID, Float>();
    predecessors = new HashMap<PRID, PVertex>();
    distance.put(paramSourceVertex.getIdentity(), MIN);
    unSettledNodes.add(paramSourceVertex);

    int maxDistances = 0;
    int maxSettled = 0;
    int maxUnSettled = 0;
    int maxPredecessors = 0;

    while (continueTraversing()) {
      final PVertex node = getMinimum(unSettledNodes);
      unSettledNodes.remove(node);
      findMinimalDistances(node);

      if (distance.size() > maxDistances)
        maxDistances = distance.size();
      if (unSettledNodes.size() > maxUnSettled)
        maxUnSettled = unSettledNodes.size();
      if (predecessors.size() > maxPredecessors)
        maxPredecessors = predecessors.size();

      if (!isVariableEdgeWeight() && distance.containsKey(paramDestinationVertex.getIdentity()))
        // FOUND
        break;
    }

    context.setVariable("maxDistances", maxDistances);
    context.setVariable("maxSettled", maxSettled);
    context.setVariable("maxUnSettled", maxUnSettled);
    context.setVariable("maxPredecessors", maxPredecessors);

    distance = null;

    return getPath();
  }

  protected boolean isVariableEdgeWeight() {
    return false;
  }

  /*
   * This method returns the path from the source to the selected target and NULL if no path exists
   */
  public LinkedList<PVertex> getPath() {
    final LinkedList<PVertex> path = new LinkedList<PVertex>();
    PVertex step = paramDestinationVertex;
    // Check if a path exists
    if (predecessors.get(step.getIdentity()) == null)
      return null;

    path.add(step);
    while (predecessors.get(step.getIdentity()) != null) {
      step = predecessors.get(step.getIdentity());
      path.add(step);
    }
    // Put it into the correct order
    Collections.reverse(path);
    return path;
  }

  public boolean aggregateResults() {
    return false;
  }

  @Override
  public Object getResult() {
    return getPath();
  }

  protected void findMinimalDistances(final PVertex node) {
    for (PVertex neighbor : getNeighbors(node)) {
      final float d = sumDistances(getShortestDistance(node), getDistance(node, neighbor));

      if (getShortestDistance(neighbor) > d) {
        distance.put(neighbor.getIdentity(), d);
        predecessors.put(neighbor.getIdentity(), node);
        unSettledNodes.add(neighbor);
      }
    }

  }

  protected Set<PVertex> getNeighbors(final PVertex node) {
    context.incrementVariable("getNeighbors");

    final Set<PVertex> neighbors = new HashSet<PVertex>();
    if (node != null) {
      for (PVertex v : node.getVertices(paramDirection)) {
        final PVertex ov = (PVertex) v;
        if (ov != null && isNotSettled(ov))
          neighbors.add(ov);
      }
    }
    return neighbors;
  }

  protected PVertex getMinimum(final Set<PVertex> vertexes) {
    PVertex minimum = null;
    Float minimumDistance = null;
    for (PVertex vertex : vertexes) {
      if (minimum == null || getShortestDistance(vertex) < minimumDistance) {
        minimum = vertex;
        minimumDistance = getShortestDistance(minimum);
      }
    }
    return minimum;
  }

  protected boolean isNotSettled(final PVertex vertex) {
    return unSettledNodes.contains(vertex) || !distance.containsKey(vertex.getIdentity());
  }

  protected boolean continueTraversing() {
    return unSettledNodes.size() > 0;
  }

  protected float getShortestDistance(final PVertex destination) {
    if (destination == null)
      return Float.MAX_VALUE;

    final Float d = distance.get(destination.getIdentity());
    return d == null ? Float.MAX_VALUE : d;
  }

  protected float sumDistances(final float iDistance1, final float iDistance2) {
    return iDistance1 + iDistance2;
  }

  protected abstract float getDistance(final PVertex node, final PVertex target);
}
