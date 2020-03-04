/*
 * Copyright (c) - Arcade Data LTD (https://arcadedata.com)
 */

package com.arcadedb.sql.executor;

/**
 * Created by luigidellaquila on 17/10/16.
 */
public class OptionalMatchStep extends MatchStep{
  public OptionalMatchStep(CommandContext context, EdgeTraversal edge, boolean profilingEnabled) {
    super(context, edge, profilingEnabled);
  }

  @Override protected MatchEdgeTraverser createTraverser(Result lastUpstreamRecord) {
    return new OptionalMatchEdgeTraverser(lastUpstreamRecord, edge);
  }



  @Override public String prettyPrint(int depth, int indent) {
    String spaces = ExecutionStepInternal.getIndent(depth, indent);
    StringBuilder result = new StringBuilder();
    result.append(spaces);
    result.append("+ OPTIONAL MATCH ");
    if (edge.out) {
      result.append(" ---->\n");
    } else {
      result.append("     <----\n");
    }
    result.append(spaces);
    result.append("  ");
    result.append("{" + edge.edge.out.alias + "}");
    result.append(edge.edge.item.getMethod());
    result.append("{" + edge.edge.in.alias + "}");
    return result.toString();
  }
}
