package com.arcadedb.sql.executor;

import com.arcadedb.exception.CommandExecutionException;
import com.arcadedb.exception.TimeoutException;
import com.arcadedb.sql.parser.Identifier;
import com.arcadedb.sql.parser.LocalResultSet;
import com.arcadedb.sql.parser.Statement;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Created by luigidellaquila on 03/08/16.
 */
public class LetQueryStep extends AbstractExecutionStep {

  private final Identifier varName;
  private final Statement  query;

  public LetQueryStep(Identifier varName, Statement query, CommandContext ctx, boolean profilingEnabled) {
    super(ctx, profilingEnabled);
    this.varName = varName;
    this.query = query;
  }

  @Override
  public ResultSet syncPull(CommandContext ctx, int nRecords) throws TimeoutException {
    if (!getPrev().isPresent()) {
      throw new CommandExecutionException("Cannot execute a local LET on a query without a target");
    }
    return new ResultSet() {
      ResultSet source = getPrev().get().syncPull(ctx, nRecords);

      @Override
      public boolean hasNext() {
        return source.hasNext();
      }

      @Override
      public Result next() {
        ResultInternal result = (ResultInternal) source.next();
        if (result != null) {
          calculate(result, ctx);
        }
        return result;
      }

      private void calculate(ResultInternal result, CommandContext ctx) {
        BasicCommandContext subCtx = new BasicCommandContext();
        subCtx.setDatabase(ctx.getDatabase());
        subCtx.setParentWithoutOverridingChild(ctx);
        InternalExecutionPlan subExecutionPlan = query.createExecutionPlan(subCtx, profilingEnabled);
        result.setMetadata(varName.getStringValue(), toList(new LocalResultSet(subExecutionPlan)));
      }

      private List<Result> toList(LocalResultSet oLocalResultSet) {
        List<Result> result = new ArrayList<>();
        while (oLocalResultSet.hasNext()) {
          result.add(oLocalResultSet.next());
        }
        oLocalResultSet.close();
        return result;
      }

      @Override
      public void close() {
        source.close();
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
    return spaces + "+ LET (for each record)\n" + spaces + "  " + varName + " = (" + query + ")";
  }
}
