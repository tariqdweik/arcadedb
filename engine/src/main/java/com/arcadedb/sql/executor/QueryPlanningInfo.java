/*
 * Copyright (c) - Arcade Data LTD (https://arcadedata.com)
 */

package com.arcadedb.sql.executor;

import com.arcadedb.sql.parser.*;

import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Created by luigidellaquila on 19/06/17.
 */
public class QueryPlanningInfo {

  protected Timeout timeout;
  boolean distinct = false;
  boolean expand   = false;

  Projection preAggregateProjection;
  Projection aggregateProjection;
  Projection projection             = null;
  Projection projectionAfterOrderBy = null;

  LetClause globalLetClause  = null;
  boolean   globalLetPresent = false;

  LetClause perRecordLetClause = null;

  /**
   * in a sharded execution plan, this maps the single server to the clusters it will be queried for to execute the query.
   */
  Map<String, Set<String>> serverToClusters;

  Map<String, SelectExecutionPlan> distributedFetchExecutionPlans;

  /**
   * set to true when the distributedFetchExecutionPlans are aggregated in the main execution plan
   */
  public boolean distributedPlanCreated = false;

  FromClause     target;
  WhereClause    whereClause;
  List<AndBlock> flattenedWhereClause;
  GroupBy        groupBy;
  OrderBy        orderBy;
  Unwind         unwind;
  Skip           skip;
  Limit          limit;

  boolean orderApplied          = false;
  boolean projectionsCalculated = false;

  AndBlock ridRangeConditions;
//  OStorage.LOCKING_STRATEGY lockRecord;

  public QueryPlanningInfo copy() {
    //TODO check what has to be copied and what can be just referenced as it is
    QueryPlanningInfo result = new QueryPlanningInfo();
    result.distinct = this.distinct;
    result.expand = this.expand;
    result.preAggregateProjection = this.preAggregateProjection;
    result.aggregateProjection = this.aggregateProjection;
    result.projection = this.projection;
    result.projectionAfterOrderBy = this.projectionAfterOrderBy;
    result.globalLetClause = this.globalLetClause;
    result.globalLetPresent = this.globalLetPresent;
    result.perRecordLetClause = this.perRecordLetClause;
    result.serverToClusters = this.serverToClusters;

//    Map<String, OSelectExecutionPlan> distributedFetchExecutionPlans;//TODO!

    result.distributedPlanCreated = this.distributedPlanCreated;
    result.target = this.target;
    result.whereClause = this.whereClause;
    result.flattenedWhereClause = this.flattenedWhereClause;
    result.groupBy = this.groupBy;
    result.orderBy = this.orderBy;
    result.unwind = this.unwind;
    result.skip = this.skip;
    result.limit = this.limit;
    result.orderApplied = this.orderApplied;
    result.projectionsCalculated = this.projectionsCalculated;
    result.ridRangeConditions = this.ridRangeConditions;

//    result.lockRecord = this.lockRecord;
    return result;
  }
}
