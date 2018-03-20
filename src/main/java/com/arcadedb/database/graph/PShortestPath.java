package com.arcadedb.database.graph;

/**
 * Shortest path algorithm to find the shortest path from one node to another node in a directed graph.
 */
public class PShortestPath {
//  private class OShortestPathContext {
//    PVertex sourceVertex;
//    PVertex destinationVertex;
//    PVertex.DIRECTION directionLeft  = PVertex.DIRECTION.BOTH;
//    PVertex.DIRECTION directionRight = PVertex.DIRECTION.BOTH;
//
//    String   edgeType;
//    String[] edgeTypeParam;
//
//    ArrayDeque<PVertex> queueLeft  = new ArrayDeque<>();
//    ArrayDeque<PVertex> queueRight = new ArrayDeque<>();
//
//    final Set<PRID> leftVisited  = new HashSet<PRID>();
//    final Set<PRID> rightVisited = new HashSet<PRID>();
//
//    final Map<PRID, PRID> previouses = new HashMap<PRID, PRID>();
//    final Map<PRID, PRID> nexts      = new HashMap<PRID, PRID>();
//
//    PVertex current;
//    PVertex currentRight;
//    public Integer maxDepth;
//    /**
//     * option that decides whether or not to return the edge information
//     */
//    public Boolean edge;
//  }
//
//  public List<PRID> execute(Object iThis, final PIdentifiable iCurrentRecord, final Object iCurrentResult, final Object[] iParams,
//      final OCommandContext iContext) {
//
//    final PRecord record = iCurrentRecord != null ? iCurrentRecord.getRecord() : null;
//
//    final OShortestPathContext ctx = new OShortestPathContext();
//
//    Object source = iParams[0];
//    if (OMultiValue.isMultiValue(source)) {
//      if (OMultiValue.getSize(source) > 1)
//        throw new IllegalArgumentException("Only one sourceVertex is allowed");
//      source = OMultiValue.getFirstValue(source);
//      if (source instanceof OResult && ((OResult) source).isElement()) {
//        source = ((OResult) source).getElement().get();
//      }
//    }
//    source = OSQLHelper.getValue(source, record, iContext);
//    if (source instanceof PIdentifiable) {
//      OElement elem = ((PIdentifiable) source).getRecord();
//      if (!elem.isVertex()) {
//        throw new IllegalArgumentException("The sourceVertex must be a vertex record");
//      }
//      ctx.sourceVertex = elem.asVertex().get();
//    } else {
//      throw new IllegalArgumentException("The sourceVertex must be a vertex record");
//    }
//
//    Object dest = iParams[1];
//    if (OMultiValue.isMultiValue(dest)) {
//      if (OMultiValue.getSize(dest) > 1)
//        throw new IllegalArgumentException("Only one destinationVertex is allowed");
//      dest = OMultiValue.getFirstValue(dest);
//      if (dest instanceof OResult && ((OResult) dest).isElement()) {
//        dest = ((OResult) dest).getElement().get();
//      }
//    }
//    dest = OSQLHelper.getValue(dest, record, iContext);
//    if (dest instanceof PIdentifiable) {
//      OElement elem = ((PIdentifiable) dest).getRecord();
//      if (!elem.isVertex()) {
//        throw new IllegalArgumentException("The destinationVertex must be a vertex record");
//      }
//      ctx.destinationVertex = elem.asVertex().get();
//    } else {
//      throw new IllegalArgumentException("The destinationVertex must be a vertex record");
//    }
//
//    if (ctx.sourceVertex.equals(ctx.destinationVertex)) {
//      final List<PRID> result = new ArrayList<PRID>(1);
//      result.add(ctx.destinationVertex.getIdentity());
//      return result;
//    }
//
//    if (iParams.length > 2 && iParams[2] != null) {
//      ctx.directionLeft = PVertex.DIRECTION.valueOf(iParams[2].toString().toUpperCase(Locale.ENGLISH));
//    }
//    if (ctx.directionLeft == PVertex.DIRECTION.OUT) {
//      ctx.directionRight = PVertex.DIRECTION.IN;
//    } else if (ctx.directionLeft == PVertex.DIRECTION.IN) {
//      ctx.directionRight = PVertex.DIRECTION.OUT;
//    }
//
//    ctx.edgeType = null;
//    if (iParams.length > 3) {
//      ctx.edgeType = iParams[3] == null ? null : "" + iParams[3];
//    }
//    ctx.edgeTypeParam = new String[] { ctx.edgeType };
//
//    if (iParams.length > 4) {
//      bindAdditionalParams(iParams[4], ctx);
//    }
//
//    ctx.queueLeft.add(ctx.sourceVertex);
//    ctx.leftVisited.add(ctx.sourceVertex.getIdentity());
//
//    ctx.queueRight.add(ctx.destinationVertex);
//    ctx.rightVisited.add(ctx.destinationVertex.getIdentity());
//
//    int depth = 1;
//    while (true) {
//      if (ctx.maxDepth != null && ctx.maxDepth <= depth) {
//        break;
//      }
//      if (ctx.queueLeft.isEmpty() || ctx.queueRight.isEmpty())
//        break;
//
//      if (Thread.interrupted())
//        throw new PGraphAlgorithmException("The shortestPath() function has been interrupted");
//
//      if (!OCommandExecutorAbstract.checkInterruption(iContext))
//        break;
//
//      List<PRID> neighbPRIDentity;
//
//      if (ctx.queueLeft.size() <= ctx.queueRight.size()) {
//        // START EVALUATING FROM LEFT
//        neighbPRIDentity = walkLeft(ctx);
//        if (neighbPRIDentity != null)
//          return neighbPRIDentity;
//        depth++;
//        if (ctx.maxDepth != null && ctx.maxDepth <= depth) {
//          break;
//        }
//
//        if (ctx.queueLeft.isEmpty())
//          break;
//
//        neighbPRIDentity = walkRight(ctx);
//        if (neighbPRIDentity != null)
//          return neighbPRIDentity;
//
//      } else {
//
//        // START EVALUATING FROM RIGHT
//        neighbPRIDentity = walkRight(ctx);
//        if (neighbPRIDentity != null)
//          return neighbPRIDentity;
//
//        depth++;
//        if (ctx.maxDepth != null && ctx.maxDepth <= depth) {
//          break;
//        }
//
//        if (ctx.queueRight.isEmpty())
//          break;
//
//        neighbPRIDentity = walkLeft(ctx);
//        if (neighbPRIDentity != null)
//          return neighbPRIDentity;
//      }
//
//      depth++;
//    }
//    return new ArrayList<PRID>();
//
//  }
//
//  private void bindAdditionalParams(Object additionalParams, OShortestPathContext ctx) {
//    if (additionalParams == null) {
//      return;
//    }
//    Map<String, Object> mapParams = null;
//    if (additionalParams instanceof Map) {
//      mapParams = (Map) additionalParams;
//    } else if (additionalParams instanceof PIdentifiable) {
//      mapParams = ((ODocument) ((PIdentifiable) additionalParams).getRecord()).toMap();
//    }
//    if (mapParams != null) {
//      ctx.maxDepth = integer(mapParams.get("maxDepth"));
//      Boolean withEdge = toBoolean(mapParams.get("edge"));
//      ctx.edge = Boolean.TRUE.equals(withEdge) ? Boolean.TRUE : Boolean.FALSE;
//    }
//  }
//
//  private Integer integer(Object fromObject) {
//    if (fromObject == null) {
//      return null;
//    }
//    if (fromObject instanceof Number) {
//      return ((Number) fromObject).intValue();
//    }
//    if (fromObject instanceof String) {
//      try {
//        return Integer.parseInt(fromObject.toString());
//      } catch (NumberFormatException ignore) {
//      }
//    }
//    return null;
//  }
//
//  /**
//   * @return
//   *
//   * @author Thomas Young (YJJThomasYoung@hotmail.com)
//   */
//  private Boolean toBoolean(Object fromObject) {
//    if (fromObject == null) {
//      return null;
//    }
//    if (fromObject instanceof Boolean) {
//      return (Boolean) fromObject;
//    }
//    if (fromObject instanceof String) {
//      try {
//        return Boolean.parseBoolean(fromObject.toString());
//      } catch (NumberFormatException ignore) {
//      }
//    }
//    return null;
//  }
//
//  /**
//   * get adjacent vertices and edges
//   *
//   * @param srcVertex
//   * @param direction
//   *
//   * @return
//   *
//   * @author Thomas Young (YJJThomasYoung@hotmail.com)
//   */
//  private PPair<Iterable<PVertex>, Iterable<PEdge>> getVerticesAndEdges(final PVertex srcVertex, final PVertex.DIRECTION direction,
//      final String type) {
//    if (direction == PVertex.DIRECTION.BOTH) {
//      final PPair<Iterable<PVertex>, Iterable<PEdge>> pair1 = getVerticesAndEdges(srcVertex, PVertex.DIRECTION.OUT, type);
//      final PPair<Iterable<PVertex>, Iterable<PEdge>> pair2 = getVerticesAndEdges(srcVertex, PVertex.DIRECTION.IN, type);
//
//      PMultiIterator<PVertex> vertexIterator = new PMultiIterator<>();
//      PMultiIterator<PEdge> edgeIterator = new PMultiIterator<>();
//
//      vertexIterator.add(pair1.getFirst());
//      vertexIterator.add(pair2.getFirst());
//      edgeIterator.add(pair1.getSecond());
//      edgeIterator.add(pair2.getSecond());
//
//      return new PPair<>(vertexIterator, edgeIterator);
//    } else {
//      final Iterable<PEdge> edges1 = srcVertex.getEdges(direction, type);
//      final Iterable<PEdge> edges2 = srcVertex.getEdges(direction, type);
//      return new PPair<>(new PEdgeTPVertexIterable(edges1, direction), edges2);
//    }
//  }
//
//  public String getSyntax() {
//    return "shortestPath(<sourceVertex>, <destinationVertex>, [<direction>, [ <edgeTypeAsString> ]])";
//  }
//
//  protected List<PRID> walkLeft(final PShortestPath.OShortestPathContext ctx) {
//    ArrayDeque<PVertex> nextLevelQueue = new ArrayDeque<>();
//    if (!Boolean.TRUE.equals(ctx.edge)) {
//      while (!ctx.queueLeft.isEmpty()) {
//        ctx.current = ctx.queueLeft.poll();
//
//        for (Iterator<PGraphCursorEntry> neighbors = ctx.current.getConnectedVertices(ctx.directionLeft, ctx.edgeType); neighbors
//            .hasNext(); ) {
//          final PRID neighbPRIDentity = neighbors.next().getConnectedVertex().getIdentity();
//
//          if (ctx.rightVisited.contains(neighbPRIDentity)) {
//            ctx.previouses.put(neighbPRIDentity, ctx.current.getIdentity());
//            return computePath(ctx.previouses, ctx.nexts, neighbPRIDentity);
//          }
//          if (!ctx.leftVisited.contains(neighbPRIDentity)) {
//            ctx.previouses.put(neighbPRIDentity, ctx.current.getIdentity());
//
//            nextLevelQueue.offer((PVertex) neighbPRIDentity.getRecord());
//            ctx.leftVisited.add(neighbPRIDentity);
//          }
//
//        }
//      }
//    } else {
//      while (!ctx.queueLeft.isEmpty()) {
//        ctx.current = ctx.queueLeft.poll();
//
//        final PPair<Iterable<PVertex>, Iterable<PEdge>> neighbors = getVerticesAndEdges(ctx.current, ctx.directionLeft,
//            ctx.edgeType);
//        final Iterator<PVertex> vertexIterator = neighbors.getFirst().iterator();
//        final Iterator<PEdge> edgeIterator = neighbors.getSecond().iterator();
//        while (vertexIterator.hasNext() && edgeIterator.hasNext()) {
//          final PVertex v = vertexIterator.next();
//          final PRID neighborVertexIdentity = v.getIdentity();
//          final PRID neighborEdgeIdentity = edgeIterator.next().getIdentity();
//
//          if (ctx.rightVisited.contains(neighborVertexIdentity)) {
//            ctx.previouses.put(neighborVertexIdentity, neighborEdgeIdentity);
//            ctx.previouses.put(neighborEdgeIdentity, ctx.current.getIdentity());
//            return computePath(ctx.previouses, ctx.nexts, neighborVertexIdentity);
//          }
//          if (!ctx.leftVisited.contains(neighborVertexIdentity)) {
//            ctx.previouses.put(neighborVertexIdentity, neighborEdgeIdentity);
//            ctx.previouses.put(neighborEdgeIdentity, ctx.current.getIdentity());
//
//            nextLevelQueue.offer(v);
//            ctx.leftVisited.add(neighborVertexIdentity);
//          }
//        }
//      }
//    }
//    ctx.queueLeft = nextLevelQueue;
//    return null;
//  }
//
//  protected List<PRID> walkRight(final PShortestPath.OShortestPathContext ctx) {
//    final ArrayDeque<PVertex> nextLevelQueue = new ArrayDeque<>();
//    if (!Boolean.TRUE.equals(ctx.edge)) {
//      while (!ctx.queueRight.isEmpty()) {
//        ctx.currentRight = ctx.queueRight.poll();
//
//        for (Iterator<PGraphCursorEntry> neighbors = ctx.currentRight
//            .getConnectedVertices(ctx.directionRight, ctx.edgeType); neighbors.hasNext(); ) {
//          final PRID neighbPRIDentity = neighbors.next().getConnectedVertex().getIdentity();
//
//          if (ctx.leftVisited.contains(neighbPRIDentity)) {
//            ctx.nexts.put(neighbPRIDentity, ctx.currentRight.getIdentity());
//            return computePath(ctx.previouses, ctx.nexts, neighbPRIDentity);
//          }
//          if (!ctx.rightVisited.contains(neighbPRIDentity)) {
//
//            ctx.nexts.put(neighbPRIDentity, ctx.currentRight.getIdentity());
//
//            nextLevelQueue.offer((PVertex) neighbPRIDentity.getRecord());
//            ctx.rightVisited.add(neighbPRIDentity);
//          }
//
//        }
//      }
//    } else {
//      while (!ctx.queueRight.isEmpty()) {
//        ctx.currentRight = ctx.queueRight.poll();
//
//        PPair<Iterable<PVertex>, Iterable<PEdge>> neighbors = getVerticesAndEdges(ctx.currentRight, ctx.directionRight,
//            ctx.edgeType);
//
//        Iterator<PVertex> vertexIterator = neighbors.getFirst().iterator();
//        Iterator<PEdge> edgeIterator = neighbors.getSecond().iterator();
//        while (vertexIterator.hasNext() && edgeIterator.hasNext()) {
//          final PVertex v = vertexIterator.next();
//          final PRID neighborVertexIdentity = v.getIdentity();
//          final PRID neighborEdgeIdentity = edgeIterator.next().getIdentity();
//
//          if (ctx.leftVisited.contains(neighborVertexIdentity)) {
//            ctx.nexts.put(neighborVertexIdentity, neighborEdgeIdentity);
//            ctx.nexts.put(neighborEdgeIdentity, ctx.currentRight.getIdentity());
//            return computePath(ctx.previouses, ctx.nexts, neighborVertexIdentity);
//          }
//          if (!ctx.rightVisited.contains(neighborVertexIdentity)) {
//            ctx.nexts.put(neighborVertexIdentity, neighborEdgeIdentity);
//            ctx.nexts.put(neighborEdgeIdentity, ctx.currentRight.getIdentity());
//
//            nextLevelQueue.offer(v);
//            ctx.rightVisited.add(neighborVertexIdentity);
//          }
//        }
//      }
//    }
//    ctx.queueRight = nextLevelQueue;
//    return null;
//  }
//
//  private List<PRID> computePath(final Map<PRID, PRID> leftDistances, final Map<PRID, PRID> rightDistances, final PRID neighbor) {
//    final List<PRID> result = new ArrayList<PRID>();
//
//    PRID current = neighbor;
//    while (current != null) {
//      result.add(0, current);
//      current = leftDistances.get(current);
//    }
//
//    current = neighbor;
//    while (current != null) {
//      current = rightDistances.get(current);
//      if (current != null) {
//        result.add(current);
//      }
//    }
//
//    return result;
//  }
}