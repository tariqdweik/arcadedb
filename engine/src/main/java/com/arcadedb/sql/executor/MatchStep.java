/*
 * Copyright (c) - Arcade Data LTD (https://arcadedata.com)
 */

package com.arcadedb.sql.executor;

import com.arcadedb.exception.TimeoutException;
import com.arcadedb.sql.parser.MultiMatchPathItem;

import java.util.Map;
import java.util.Optional;

/**
 * @author Luigi Dell'Aquila
 */
public class MatchStep extends AbstractExecutionStep {
  protected final EdgeTraversal edge;

  ResultSet          upstream;
  Result             lastUpstreamRecord;
  MatchEdgeTraverser traverser;
  Result             nextResult;

  public MatchStep(CommandContext context, EdgeTraversal edge, boolean profilingEnabled) {
    super(context, profilingEnabled);
    this.edge = edge;
  }

  @Override
  public void reset() {
    this.upstream = null;
    this.lastUpstreamRecord = null;
    this.traverser = null;
    this.nextResult = null;
  }

  @Override
  public ResultSet syncPull(CommandContext ctx, int nRecords) throws TimeoutException {
    return new ResultSet() {
      int localCount = 0;

      @Override
      public boolean hasNext() {
        if (localCount >= nRecords) {
          return false;
        }
        if (nextResult == null) {
          fetchNext(ctx, nRecords);
        }
        return nextResult != null;
      }

      @Override
      public Result next() {
        if (localCount >= nRecords) {
          throw new IllegalStateException();
        }
        if (nextResult == null) {
          fetchNext(ctx, nRecords);
        }
        if (nextResult == null) {
          throw new IllegalStateException();
        }
        Result result = nextResult;
        fetchNext(ctx, nRecords);
        localCount++;
        ctx.setVariable("$matched", result);
        return result;
      }

      @Override
      public void close() {

      }

      @Override
      public Optional<ExecutionPlan> getExecutionPlan() {
        return null;
      }

      @Override
      public Map<String, Long> getQueryStats() {
        return null;
      }
    };
  }

  private void fetchNext(CommandContext ctx, int nRecords) {
    nextResult = null;
    while (true) {
      if (traverser != null && traverser.hasNext(ctx)) {
        nextResult = traverser.next(ctx);
        break;
      }

      if (upstream == null || !upstream.hasNext()) {
        upstream = getPrev().get().syncPull(ctx, nRecords);
      }
      if (!upstream.hasNext()) {
        return;
      }

      lastUpstreamRecord = upstream.next();

      traverser = createTraverser(lastUpstreamRecord);

      boolean found = false;
      while (traverser.hasNext(ctx)) {
        nextResult = traverser.next(ctx);
        if (nextResult != null) {
          found = true;
          break;
        }
      }
      if (found) {
        break;
      }
    }
  }

  protected MatchEdgeTraverser createTraverser(Result lastUpstreamRecord) {
    if (edge.edge.item instanceof MultiMatchPathItem) {
      return new MatchMultiEdgeTraverser(lastUpstreamRecord, edge);
    } else if (edge.out) {
      return new MatchEdgeTraverser(lastUpstreamRecord, edge);
    } else {
      return new MatchReverseEdgeTraverser(lastUpstreamRecord, edge);
    }
  }

  @Override
  public String prettyPrint(int depth, int indent) {
    String spaces = ExecutionStepInternal.getIndent(depth, indent);
    StringBuilder result = new StringBuilder();
    result.append(spaces);
    result.append("+ MATCH ");
    if (edge.out) {
      result.append("     ---->\n");
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
