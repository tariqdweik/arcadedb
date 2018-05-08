package com.arcadedb.sql.executor;

import com.arcadedb.exception.TimeoutException;
import com.arcadedb.sql.parser.Identifier;

import java.util.Map;
import java.util.Optional;

/**
 * @author Luigi Dell'Aquila (l.dellaquila-(at)-orientdb.com)
 */
public class RemoveEmptyOptionalsStep extends AbstractExecutionStep {

  public RemoveEmptyOptionalsStep(CommandContext ctx, Identifier cluster, boolean profilingEnabled) {
    super(ctx, profilingEnabled);

  }

  public RemoveEmptyOptionalsStep(CommandContext ctx, boolean profilingEnabled) {
    this(ctx, null, profilingEnabled);
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
        ResultInternal result = (ResultInternal) upstream.next();
        for (String s : result.getPropertyNames()) {
          if (OptionalMatchEdgeTraverser.isEmptyOptional(result.getProperty(s))) {
            result.setProperty(s, null);
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
    result.append("+ REMOVE EMPTY OPTIONALS");
    return result.toString();
  }
}
