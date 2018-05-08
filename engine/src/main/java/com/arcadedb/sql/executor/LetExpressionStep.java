package com.arcadedb.sql.executor;

import com.arcadedb.exception.CommandExecutionException;
import com.arcadedb.exception.TimeoutException;
import com.arcadedb.sql.parser.Expression;
import com.arcadedb.sql.parser.Identifier;

import java.util.Map;
import java.util.Optional;

/**
 * Created by luigidellaquila on 03/08/16.
 */
public class LetExpressionStep extends AbstractExecutionStep {
  private Identifier varname;
  private Expression expression;

  public LetExpressionStep(Identifier varName, Expression expression, CommandContext ctx, boolean profilingEnabled) {
    super(ctx, profilingEnabled);
    this.varname = varName;
    this.expression = expression;
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
        Object value = expression.execute(result, ctx);
        result.setMetadata(varname.getStringValue(), value);
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
    return spaces + "+ LET (for each record)\n" + spaces + "  " + varname + " = " + expression;
  }

  @Override
  public Result serialize() {
    ResultInternal result = ExecutionStepInternal.basicSerialize(this);
    if (varname != null) {
      result.setProperty("varname", varname.serialize());
    }
    if (expression != null) {
      result.setProperty("expression", expression.serialize());
    }
    return result;
  }

  @Override
  public void deserialize(Result fromResult) {
    try {
      ExecutionStepInternal.basicDeserialize(fromResult, this);
      if (fromResult.getProperty("varname") != null) {
        varname = Identifier.deserialize(fromResult.getProperty("varname"));
      }
      if (fromResult.getProperty("expression") != null) {
        expression = new Expression(-1);
        expression.deserialize(fromResult.getProperty("expression"));
      }
      reset();
    } catch (Exception e) {
      throw new CommandExecutionException(e);
    }
  }
}
