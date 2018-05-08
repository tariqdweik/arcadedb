package com.arcadedb.sql.executor;

import com.arcadedb.sql.parser.WhereClause;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 * Created by luigidellaquila on 13/10/16.
 */
public class WhileMatchStep extends AbstractUnrollStep {

  private final InternalExecutionPlan body;
  private final WhereClause           condition;

  public WhileMatchStep(CommandContext ctx, WhereClause condition, InternalExecutionPlan body, boolean profilingEnabled) {
    super(ctx, profilingEnabled);
    this.body = body;
    this.condition = condition;
  }

  @Override protected Collection<Result> unroll(Result doc, CommandContext iContext) {
    body.reset(iContext);
    List<Result> result = new ArrayList<>();
    ResultSet block = body.fetchNext(100);
    while(block.hasNext()){
      while(block.hasNext()){
        result.add(block.next());
      }
      block = body.fetchNext(100);
    }
    return result;
  }

  @Override public String prettyPrint(int depth, int indent) {
    String indentStep = ExecutionStepInternal.getIndent(1, indent);
    String spaces = ExecutionStepInternal.getIndent(depth, indent);
    StringBuilder result = new StringBuilder();
    result.append(spaces);
    result.append("+ WHILE\n");

    result.append(spaces);
    result.append(indentStep);
    result.append(condition.toString());
    result.append("\n");

    result.append(spaces);
    result.append("  DO\n");


    result.append(body.prettyPrint(depth+1, indent));
    result.append("\n");

    result.append(spaces);
    result.append("  END\n");

    return result.toString();
  }

}
