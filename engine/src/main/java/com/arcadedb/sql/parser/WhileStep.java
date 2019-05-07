package com.arcadedb.sql.parser;

import com.arcadedb.exception.TimeoutException;
import com.arcadedb.sql.executor.AbstractExecutionStep;
import com.arcadedb.sql.executor.BasicCommandContext;
import com.arcadedb.sql.executor.CommandContext;
import com.arcadedb.sql.executor.EmptyStep;
import com.arcadedb.sql.executor.ExecutionStepInternal;
import com.arcadedb.sql.executor.InternalExecutionPlan;
import com.arcadedb.sql.executor.ResultInternal;
import com.arcadedb.sql.executor.ResultSet;
import com.arcadedb.sql.executor.ScriptExecutionPlan;

import java.util.List;

public class WhileStep extends AbstractExecutionStep {
    private final BooleanExpression condition;
    private final List<Statement> statements;

    private ExecutionStepInternal finalResult = null;

    public WhileStep(BooleanExpression condition, List<Statement> statements, CommandContext ctx, boolean enableProfiling) {
        super(ctx, enableProfiling);
        this.condition = condition;
        this.statements = statements;
    }

    @Override
    public ResultSet syncPull(CommandContext ctx, int nRecords) throws TimeoutException {
        prev.ifPresent(x -> x.syncPull(ctx, nRecords));
        if (finalResult != null) {
            return finalResult.syncPull(ctx, nRecords);
        }

        while (condition.evaluate(new ResultInternal(), ctx)) {

            ScriptExecutionPlan plan = initPlan(ctx);
            ExecutionStepInternal result = plan.executeFull();
            if (result != null) {
                this.finalResult = result;
                return result.syncPull(ctx, nRecords);
            }
        }
        finalResult = new EmptyStep(ctx, false);
        return finalResult.syncPull(ctx, nRecords);

    }

    public ScriptExecutionPlan initPlan(CommandContext ctx) {
        BasicCommandContext subCtx1 = new BasicCommandContext();
        subCtx1.setParent(ctx);
        ScriptExecutionPlan plan = new ScriptExecutionPlan(subCtx1);
        for (Statement stm : statements) {
            if (stm.originalStatement == null) {
                stm.originalStatement = stm.toString();
            }
            InternalExecutionPlan subPlan = stm.createExecutionPlan(subCtx1, profilingEnabled);
            plan.chain(subPlan, profilingEnabled);
        }
        return plan;
    }

    public boolean containsReturn() {
        for (Statement stm : this.statements) {
            if (stm instanceof ReturnStatement) {
                return true;
            }
            if (stm instanceof ForEachBlock && ((ForEachBlock) stm).containsReturn()) {
                return true;
            }
            if (stm instanceof IfStatement && ((IfStatement) stm).containsReturn()) {
                return true;
            }
            if (stm instanceof WhileBlock && ((WhileBlock) stm).containsReturn()) {
                return true;
            }
        }
        return false;
    }
}
