package com.arcadedb.sql.executor;

import com.arcadedb.database.PIdentifiable;
import com.arcadedb.sql.parser.OMatchPathItem;
import com.arcadedb.sql.parser.OMatchStatement;

/**
 * Created by luigidellaquila on 28/07/15.
 */
public class PatternEdge {
  public PatternNode    in;
  public PatternNode    out;
  public OMatchPathItem item;

  public Iterable<PIdentifiable> executeTraversal(OMatchStatement.MatchContext matchContext, OCommandContext iCommandContext,
      PIdentifiable startingPoint, int depth) {
    return item.executeTraversal(matchContext, iCommandContext, startingPoint, depth);
  }

  @Override
  public String toString() {
    return "{as: "+in.alias+"}"+item.toString();
  }
}
