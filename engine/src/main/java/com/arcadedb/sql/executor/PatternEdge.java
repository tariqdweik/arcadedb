/*
 * Copyright (c) 2018 - Arcade Analytics LTD (https://arcadeanalytics.com)
 */

package com.arcadedb.sql.executor;

import com.arcadedb.database.Identifiable;
import com.arcadedb.sql.parser.MatchPathItem;
import com.arcadedb.sql.parser.MatchStatement;

/**
 * Created by luigidellaquila on 28/07/15.
 */
public class PatternEdge {
  public PatternNode   in;
  public PatternNode   out;
  public MatchPathItem item;

  public Iterable<Identifiable> executeTraversal(MatchStatement.MatchContext matchContext, CommandContext iCommandContext,
      Identifiable startingPoint, int depth) {
    return item.executeTraversal(matchContext, iCommandContext, startingPoint, depth);
  }

  @Override
  public String toString() {
    return "{as: "+in.alias+"}"+item.toString();
  }
}
