package com.arcadedb.sql.executor;

import com.arcadedb.exception.TimeoutException;
import com.arcadedb.sql.parser.Expression;
import com.arcadedb.sql.parser.ForEachBlock;
import com.arcadedb.sql.parser.Identifier;
import com.arcadedb.sql.parser.IfStatement;
import com.arcadedb.sql.parser.ReturnStatement;
import com.arcadedb.sql.parser.Statement;

import java.util.Iterator;
import java.util.List;

/**
 * Created by luigidellaquila on 19/09/16.
 */
public class ForEachStep extends AbstractExecutionStep {
    private final Identifier loopVariable;
    private final Expression source;
    public List<Statement> body;

    Iterator iterator;
    private ExecutionStepInternal finalResult = null;
    private boolean inited = false;

    public ForEachStep(Identifier loopVariable, Expression oExpression, List<Statement> statements, CommandContext ctx,
                       boolean enableProfiling) {
        super(ctx, enableProfiling);
        this.loopVariable = loopVariable;
        this.source = oExpression;
        this.body = statements;
    }

    @Override
    public ResultSet syncPull(CommandContext ctx, int nRecords) throws TimeoutException {
        prev.get().syncPull(ctx, nRecords);
        if (finalResult != null) {
            return finalResult.syncPull(ctx, nRecords);
        }
        init(ctx);
        while (iterator.hasNext()) {
            ctx.setVariable(loopVariable.getStringValue(), iterator.next());
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

    protected void init(CommandContext ctx) {
        if (!this.inited) {
            Object val = source.execute(new ResultInternal(), ctx);
            this.iterator = MultiValue.getMultiValueIterator(val);
            this.inited = true;
        }
    }

    public ScriptExecutionPlan initPlan(CommandContext ctx) {
        BasicCommandContext subCtx1 = new BasicCommandContext();
        subCtx1.setParent(ctx);
        ScriptExecutionPlan plan = new ScriptExecutionPlan(subCtx1);
        for (Statement stm : body) {
            plan.chain(stm.createExecutionPlan(subCtx1, profilingEnabled), profilingEnabled);
        }
        return plan;
    }

    public boolean containsReturn() {
        for (Statement stm : this.body) {
            if (stm instanceof ReturnStatement) {
                return true;
            }
            if (stm instanceof ForEachBlock && ((ForEachBlock) stm).containsReturn()) {
                return true;
            }
            if (stm instanceof IfStatement && ((IfStatement) stm).containsReturn()) {
                return true;
            }
        }
        return false;
    }
}
