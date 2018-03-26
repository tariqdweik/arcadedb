package com.arcadedb.sql.executor;

import com.arcadedb.database.PIdentifiable;
import com.arcadedb.database.PRecord;
import com.arcadedb.sql.parser.OMatchPathItem;
import com.arcadedb.sql.parser.ORid;
import com.arcadedb.sql.parser.OWhereClause;

import java.util.*;

/**
 * Created by luigidellaquila on 23/09/16.
 */
public class MatchEdgeTraverser {
  protected OResult        sourceRecord;
  protected EdgeTraversal  edge;
  protected OMatchPathItem item;

  Iterator<OResultInternal> downstream;

  public MatchEdgeTraverser(OResult lastUpstreamRecord, EdgeTraversal edge) {
    this.sourceRecord = lastUpstreamRecord;
    this.edge = edge;
    this.item = edge.edge.item;
  }

  public MatchEdgeTraverser(OResult lastUpstreamRecord, OMatchPathItem item) {
    this.sourceRecord = lastUpstreamRecord;
    this.item = item;
  }

  public boolean hasNext(OCommandContext ctx) {
    init(ctx);
    return downstream.hasNext();
  }

  public OResult next(OCommandContext ctx) {
    init(ctx);
    if (!downstream.hasNext()) {
      throw new IllegalStateException();
    }
    String endPointAlias = getEndpointAlias();
    OResultInternal nextR = downstream.next();
    PRecord nextElement = nextR.getElement().get();
    Object prevValue = sourceRecord.getProperty(endPointAlias);
    if (prevValue != null && !equals(prevValue, nextElement)) {
      return null;
    }
    OResultInternal result = new OResultInternal();
    for (String prop : sourceRecord.getPropertyNames()) {
      result.setProperty(prop, sourceRecord.getProperty(prop));
    }
    result.setProperty(endPointAlias, toResult(nextElement));
    if (edge.edge.item.getFilter().getDepthAlias() != null) {
      result.setProperty(edge.edge.item.getFilter().getDepthAlias(), nextR.getMetadata("$depth"));
    }
    if (edge.edge.item.getFilter().getPathAlias() != null) {
      result.setProperty(edge.edge.item.getFilter().getPathAlias(), nextR.getMetadata("$matchPath"));
    }
    return result;
  }

  protected boolean equals(Object prevValue, PIdentifiable nextElement) {
    if (prevValue instanceof OResult) {
      prevValue = ((OResult) prevValue).getElement().orElse(null);
    }
    if (nextElement instanceof OResult) {
      nextElement = ((OResult) nextElement).getElement().orElse(null);
    }
    return prevValue != null && prevValue.equals(nextElement);
  }

  protected Object toResult(PRecord nextElement) {
    OResultInternal result = new OResultInternal();
    result.setElement(nextElement);
    return result;
  }

  protected String getStartingPointAlias() {
    return this.edge.edge.out.alias;
  }

  protected String getEndpointAlias() {
    if (this.item != null) {
      return this.item.getFilter().getAlias();
    }
    return this.edge.edge.in.alias;
  }

  protected void init(OCommandContext ctx) {
    if (downstream == null) {
      Object startingElem = sourceRecord.getProperty(getStartingPointAlias());
      if (startingElem instanceof OResult) {
        startingElem = ((OResult) startingElem).getElement().orElse(null);
      }
      downstream = executeTraversal(ctx, this.item, (PIdentifiable) startingElem, 0, null).iterator();
    }
  }

  protected Iterable<OResultInternal> executeTraversal(OCommandContext iCommandContext, OMatchPathItem item,
      PIdentifiable startingPoint, int depth, List<PIdentifiable> pathToHere) {

    OWhereClause filter = null;
    OWhereClause whileCondition = null;
    Integer maxDepth = null;
    String className = null;
    Integer clusterId = null;
    ORid targetRid = null;
    if (item.getFilter() != null) {
      filter = getTargetFilter(item);
      whileCondition = item.getFilter().getWhileCondition();
      maxDepth = item.getFilter().getMaxDepth();
      className = targetClassName(item, iCommandContext);
      String clusterName = targetClusterName(item, iCommandContext);
      if (clusterName != null) {
        clusterId = iCommandContext.getDatabase().getSchema().getBucketByName(clusterName).getId();
      }
      targetRid = targetRid(item, iCommandContext);
    }

    List<OResultInternal> result = new ArrayList<>();

    if (whileCondition == null && maxDepth == null) {// in this case starting point is not returned and only one level depth is
      // evaluated
      Iterable<OResultInternal> queryResult = traversePatternEdge(startingPoint, iCommandContext);

      for (OResultInternal origin : queryResult) {
        Object previousMatch = iCommandContext.getVariable("$currentMatch");
        PRecord elem = origin.toElement();
        iCommandContext.setVariable("$currentMatch", elem);
        if (matchesFilters(iCommandContext, filter, elem) && matchesClass(iCommandContext, className, elem) && matchesCluster(
            iCommandContext, clusterId, elem) && matchesRid(iCommandContext, targetRid, elem)) {
          result.add(origin);
        }
        iCommandContext.setVariable("$currentMatch", previousMatch);
      }
    } else {// in this case also zero level (starting point) is considered and traversal depth is given by the while condition
      iCommandContext.setVariable("$depth", depth);
      Object previousMatch = iCommandContext.getVariable("$currentMatch");
      iCommandContext.setVariable("$currentMatch", startingPoint);

      if (matchesFilters(iCommandContext, filter, startingPoint) && matchesClass(iCommandContext, className, startingPoint)
          && matchesCluster(iCommandContext, clusterId, startingPoint) && matchesRid(iCommandContext, targetRid, startingPoint)) {
        OResultInternal rs = new OResultInternal(startingPoint.getRecord());
        // set traversal depth in the metadata
        rs.setMetadata("$depth", depth);
        // set traversal path in the metadata
        rs.setMetadata("$matchPath", pathToHere == null ? Collections.EMPTY_LIST : pathToHere);
        // add the result to the list
        result.add(rs);
      }

      if ((maxDepth == null || depth < maxDepth) && (whileCondition == null || whileCondition
          .matchesFilters(startingPoint, iCommandContext))) {

        Iterable<OResultInternal> queryResult = traversePatternEdge(startingPoint, iCommandContext);

        for (OResultInternal origin : queryResult) {
          //          if(origin.equals(startingPoint)){
          //            continue;
          //          }
          // TODO consider break strategies (eg. re-traverse nodes)

          List<PIdentifiable> newPath = new ArrayList<>();
          if (pathToHere != null) {
            newPath.addAll(pathToHere);
          }

          PRecord elem = origin.toElement();
          newPath.add(elem.getIdentity());

          Iterable<OResultInternal> subResult = executeTraversal(iCommandContext, item, elem, depth + 1, newPath);
          if (subResult instanceof Collection) {
            result.addAll((Collection<? extends OResultInternal>) subResult);
          } else {
            for (OResultInternal i : subResult) {
              result.add(i);
            }
          }
        }
      }
      iCommandContext.setVariable("$currentMatch", previousMatch);
    }
    return result;
  }

  protected OWhereClause getTargetFilter(OMatchPathItem item) {
    return item.getFilter().getFilter();
  }

  protected String targetClassName(OMatchPathItem item, OCommandContext iCommandContext) {
    return item.getFilter().getClassName(iCommandContext);
  }

  protected String targetClusterName(OMatchPathItem item, OCommandContext iCommandContext) {
    return item.getFilter().getClusterName(iCommandContext);
  }

  protected ORid targetRid(OMatchPathItem item, OCommandContext iCommandContext) {
    return item.getFilter().getRid(iCommandContext);
  }

  private boolean matchesClass(OCommandContext iCommandContext, String className, PIdentifiable origin) {
    if (className == null) {
      return true;
    }
    PRecord element = null;
    if (origin instanceof PRecord) {
      element = (PRecord) origin;
    } else {
      Object record = origin.getRecord();
      if (record instanceof PRecord) {
        element = (PRecord) record;
      }
    }
    if (element != null) {
      Object clazz = element.getType();
      if (clazz == null) {
        return false;
      }
      return clazz.equals(className);
    }
    return false;
  }

  private boolean matchesCluster(OCommandContext iCommandContext, Integer clusterId, PIdentifiable origin) {
    if (clusterId == null) {
      return true;
    }
    if (origin == null) {
      return false;
    }

    if (origin.getIdentity() == null) {
      return false;
    }
    return clusterId.equals(origin.getIdentity().getBucketId());
  }

  private boolean matchesRid(OCommandContext iCommandContext, ORid rid, PIdentifiable origin) {
    if (rid == null) {
      return true;
    }
    if (origin == null) {
      return false;
    }

    if (origin.getIdentity() == null) {
      return false;
    }
    return origin.getIdentity().equals(rid.toRecordId(origin, iCommandContext));
  }

  protected boolean matchesFilters(OCommandContext iCommandContext, OWhereClause filter, PIdentifiable origin) {
    return filter == null || filter.matchesFilters(origin, iCommandContext);
  }

  //TODO refactor this method to receive the item.

  protected Iterable<OResultInternal> traversePatternEdge(PIdentifiable startingPoint, OCommandContext iCommandContext) {

    Iterable possibleResults = null;
    if (this.item.getFilter() != null) {
      String alias = getEndpointAlias();
      Object matchedNodes = iCommandContext.getVariable(MatchPrefetchStep.PREFETCHED_MATCH_ALIAS_PREFIX + alias);
      if (matchedNodes != null) {
        if (matchedNodes instanceof Iterable) {
          possibleResults = (Iterable) matchedNodes;
        } else {
          possibleResults = Collections.singleton(matchedNodes);
        }
      }
    }

    Object prevCurrent = iCommandContext.getVariable("$current");
    iCommandContext.setVariable("$current", startingPoint);
    Object qR;
    try {
      qR = this.item.getMethod().execute(startingPoint, possibleResults, iCommandContext);
    } finally {
      iCommandContext.setVariable("$current", prevCurrent);
    }

    if (qR == null) {
      return Collections.EMPTY_LIST;
    }
    if (qR instanceof PRecord) {
      return Collections.singleton(new OResultInternal((PRecord) qR));
    }
    if (qR instanceof Iterable) {
      Iterable iterable = (Iterable) qR;
      List<OResultInternal> result = new ArrayList<>();
      for (Object o : iterable) {
        if (o instanceof PRecord) {
          result.add(new OResultInternal((PRecord) o));
        } else if (o instanceof OResultInternal) {
          result.add((OResultInternal) o);
        } else if (o == null) {
          continue;
        } else {
          throw new UnsupportedOperationException();
        }
      }
      return result;
    }
    return Collections.EMPTY_LIST;
  }

}
