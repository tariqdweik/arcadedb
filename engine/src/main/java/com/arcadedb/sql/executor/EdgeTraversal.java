/*
 * Copyright (c) 2018 - Arcade Analytics LTD (https://arcadeanalytics.com)
 */

package com.arcadedb.sql.executor;

import com.arcadedb.sql.parser.Rid;
import com.arcadedb.sql.parser.WhereClause;

/**
 * Created by luigidellaquila on 20/09/16.
 */
public class EdgeTraversal {
  boolean out = true;
  public  PatternEdge edge;
  private String      leftClass;
  private String      leftCluster;
  private Rid         leftRid;
  private WhereClause leftFilter;

  public EdgeTraversal(PatternEdge edge, boolean out) {
    this.edge = edge;
    this.out = out;
  }

  public void setLeftClass(String leftClass) {
    this.leftClass = leftClass;
  }

  public void setLeftFilter(WhereClause leftFilter) {
    this.leftFilter = leftFilter;
  }

  public String getLeftClass() {
    return leftClass;
  }
  public String getLeftCluster() {
    return leftCluster;
  }

  public Rid getLeftRid() {
    return leftRid;
  }

  public void setLeftCluster(String leftCluster) {
    this.leftCluster = leftCluster;
  }

  public void setLeftRid(Rid leftRid) {
    this.leftRid = leftRid;
  }

  public WhereClause getLeftFilter() {
    return leftFilter;
  }

  @Override
  public String toString() {
    return edge.toString();
  }
}