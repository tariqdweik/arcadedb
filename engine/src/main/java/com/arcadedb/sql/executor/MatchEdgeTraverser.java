/*
 * Copyright (c) 2018 - Arcade Analytics LTD (https://arcadeanalytics.com)
 */

package com.arcadedb.sql.executor;

import com.arcadedb.database.Document;
import com.arcadedb.database.Identifiable;
import com.arcadedb.database.Record;
import com.arcadedb.sql.parser.MatchPathItem;
import com.arcadedb.sql.parser.Rid;
import com.arcadedb.sql.parser.WhereClause;

import java.util.*;

/**
 * Created by luigidellaquila on 23/09/16.
 */
public class MatchEdgeTraverser {
  protected Result        sourceRecord;
  protected EdgeTraversal edge;
  protected MatchPathItem item;

  Iterator<ResultInternal> downstream;

  public MatchEdgeTraverser(Result lastUpstreamRecord, EdgeTraversal edge) {
    this.sourceRecord = lastUpstreamRecord;
    this.edge = edge;
    this.item = edge.edge.item;
  }

  public MatchEdgeTraverser(Result lastUpstreamRecord, MatchPathItem item) {
    this.sourceRecord = lastUpstreamRecord;
    this.item = item;
  }

  public boolean hasNext(CommandContext ctx) {
    init(ctx);
    return downstream.hasNext();
  }

  public Result next(CommandContext ctx) {
    init(ctx);
    if (!downstream.hasNext()) {
      throw new IllegalStateException();
    }
    String endPointAlias = getEndpointAlias();
    ResultInternal nextR = downstream.next();
    Document nextElement = nextR.getElement().get();
    Object prevValue = sourceRecord.getProperty(endPointAlias);
    if (prevValue != null && !equals(prevValue, nextElement)) {
      return null;
    }
    ResultInternal result = new ResultInternal();
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

  protected boolean equals(Object prevValue, Identifiable nextElement) {
    if (prevValue instanceof Result) {
      prevValue = ((Result) prevValue).getElement().orElse(null);
    }
    if (nextElement instanceof Result) {
      nextElement = ((Result) nextElement).getElement().orElse(null);
    }
    return prevValue != null && prevValue.equals(nextElement);
  }

  protected Object toResult(Document nextElement) {
    ResultInternal result = new ResultInternal();
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

  protected void init(CommandContext ctx) {
    if (downstream == null) {
      Object startingElem = sourceRecord.getProperty(getStartingPointAlias());
      if (startingElem instanceof Result) {
        startingElem = ((Result) startingElem).getElement().orElse(null);
      }
      downstream = executeTraversal(ctx, this.item, (Identifiable) startingElem, 0, null).iterator();
    }
  }

  protected Iterable<ResultInternal> executeTraversal(CommandContext iCommandContext, MatchPathItem item,
      Identifiable startingPoint, int depth, List<Identifiable> pathToHere) {

    WhereClause filter = null;
    WhereClause whileCondition = null;
    Integer maxDepth = null;
    String className = null;
    Integer clusterId = null;
    Rid targetRid = null;
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

    List<ResultInternal> result = new ArrayList<>();

    if (whileCondition == null && maxDepth == null) {// in this case starting point is not returned and only one level depth is
      // evaluated
      Iterable<ResultInternal> queryResult = traversePatternEdge(startingPoint, iCommandContext);

      for (ResultInternal origin : queryResult) {
        Object previousMatch = iCommandContext.getVariable("$currentMatch");
        Record elem = origin.toElement();
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
        ResultInternal rs = new ResultInternal((Document) startingPoint.getRecord());
        // set traversal depth in the metadata
        rs.setMetadata("$depth", depth);
        // set traversal path in the metadata
        rs.setMetadata("$matchPath", pathToHere == null ? Collections.EMPTY_LIST : pathToHere);
        // add the result to the list
        result.add(rs);
      }

      if ((maxDepth == null || depth < maxDepth) && (whileCondition == null || whileCondition
          .matchesFilters(startingPoint, iCommandContext))) {

        Iterable<ResultInternal> queryResult = traversePatternEdge(startingPoint, iCommandContext);

        for (ResultInternal origin : queryResult) {
          //          if(origin.equals(startingPoint)){
          //            continue;
          //          }
          // TODO consider break strategies (eg. re-traverse nodes)

          List<Identifiable> newPath = new ArrayList<>();
          if (pathToHere != null) {
            newPath.addAll(pathToHere);
          }

          Record elem = origin.toElement();
          newPath.add(elem.getIdentity());

          Iterable<ResultInternal> subResult = executeTraversal(iCommandContext, item, elem, depth + 1, newPath);
          if (subResult instanceof Collection) {
            result.addAll((Collection<? extends ResultInternal>) subResult);
          } else {
            for (ResultInternal i : subResult) {
              result.add(i);
            }
          }
        }
      }
      iCommandContext.setVariable("$currentMatch", previousMatch);
    }
    return result;
  }

  protected WhereClause getTargetFilter(MatchPathItem item) {
    return item.getFilter().getFilter();
  }

  protected String targetClassName(MatchPathItem item, CommandContext iCommandContext) {
    return item.getFilter().getTypeName(iCommandContext);
  }

  protected String targetClusterName(MatchPathItem item, CommandContext iCommandContext) {
    return item.getFilter().getBucketName(iCommandContext);
  }

  protected Rid targetRid(MatchPathItem item, CommandContext iCommandContext) {
    return item.getFilter().getRid(iCommandContext);
  }

  private boolean matchesClass(CommandContext iCommandContext, String className, Identifiable origin) {
    if (className == null) {
      return true;
    }
    Document element = null;
    if (origin instanceof Document) {
      element = (Document) origin;
    } else {
      Object record = origin.getRecord();
      if (record instanceof Document) {
        element = (Document) record;
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

  private boolean matchesCluster(CommandContext iCommandContext, Integer clusterId, Identifiable origin) {
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

  private boolean matchesRid(CommandContext iCommandContext, Rid rid, Identifiable origin) {
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

  protected boolean matchesFilters(CommandContext iCommandContext, WhereClause filter, Identifiable origin) {
    return filter == null || filter.matchesFilters(origin, iCommandContext);
  }

  //TODO refactor this method to receive the item.

  protected Iterable<ResultInternal> traversePatternEdge(Identifiable startingPoint, CommandContext iCommandContext) {

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
    if (qR instanceof Document) {
      return Collections.singleton(new ResultInternal((Document) qR));
    }
    if (qR instanceof Iterable) {
      Iterable iterable = (Iterable) qR;
      List<ResultInternal> result = new ArrayList<>();
      for (Object o : iterable) {
        if (o instanceof Document) {
          result.add(new ResultInternal((Document) o));
        } else if (o instanceof ResultInternal) {
          result.add((ResultInternal) o);
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
