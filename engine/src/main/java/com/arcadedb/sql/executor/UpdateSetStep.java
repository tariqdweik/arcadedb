package com.arcadedb.sql.executor;

import com.arcadedb.exception.TimeoutException;
import com.arcadedb.sql.parser.UpdateItem;

import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Created by luigidellaquila on 09/08/16.
 */
public class UpdateSetStep extends AbstractExecutionStep {
  private final List<UpdateItem> items;

  public UpdateSetStep(List<UpdateItem> updateItems, CommandContext ctx, boolean profilingEnabled) {
    super(ctx, profilingEnabled);
    this.items = updateItems;
  }

  @Override
  public ResultSet syncPull(CommandContext ctx, int nRecords) throws TimeoutException {
    ResultSet upstream = getPrev().get().syncPull(ctx, nRecords);
    return new ResultSet() {
      @Override
      public boolean hasNext() {
        return upstream.hasNext();
      }

      @Override
      public Result next() {
        Result result = upstream.next();
        if (result instanceof ResultInternal) {
          for (UpdateItem item : items) {
            item.applyUpdate((ResultInternal) result, ctx);
          }
        }
        return result;
      }

      @Override
      public void close() {
        upstream.close();
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

  @Override
  public String prettyPrint(int depth, int indent) {
    String spaces = ExecutionStepInternal.getIndent(depth, indent);
    StringBuilder result = new StringBuilder();
    result.append(spaces);
    result.append("+ UPDATE SET");
    for (int i = 0; i < items.size(); i++) {
      UpdateItem item = items.get(i);
      if (i < items.size()) {
        result.append("\n");
      }
      result.append(spaces);
      result.append("  ");
      result.append(item.toString());
    }
    return result.toString();
  }
}
