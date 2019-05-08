/*
 * Copyright (c) 2018 - Arcade Analytics LTD (https://arcadeanalytics.com)
 */

package com.arcadedb.sql.executor;

import com.arcadedb.exception.TimeoutException;
import com.arcadedb.sql.parser.BooleanExpression;
import com.arcadedb.sql.parser.Statement;

import java.util.List;

/**
 * Created by luigidellaquila on 19/09/16.
 */
public class IfStep extends AbstractExecutionStep {
    public List<Statement> positiveStatements;
    public List<Statement> negativeStatements;
    BooleanExpression condition;
    ScriptExecutionPlan positivePlan;
    ScriptExecutionPlan negativePlan;
    Boolean conditionMet = null;


    public IfStep(CommandContext ctx, boolean profilingEnabled) {
        super(ctx, profilingEnabled);
    }

    @Override
    public ResultSet syncPull(CommandContext ctx, int nRecords) throws TimeoutException {
        init(ctx);
        if (conditionMet) {
            initPositivePlan(ctx);
            return positivePlan.fetchNext(nRecords);
        } else {
            initNegativePlan(ctx);
            if (negativePlan != null) {
                return negativePlan.fetchNext(nRecords);
            }
        }
        return new InternalResultSet();

    }

    protected void init(CommandContext ctx) {
        if (conditionMet == null) {
            conditionMet = condition.evaluate((Result) null, ctx);
        }
    }

    public void initPositivePlan(CommandContext ctx) {
        if (positivePlan == null) {
            BasicCommandContext subCtx1 = new BasicCommandContext();
            subCtx1.setParent(ctx);
            ScriptExecutionPlan positivePlan = new ScriptExecutionPlan(subCtx1);
            for (Statement stm : positiveStatements) {
                positivePlan.chain(stm.createExecutionPlan(subCtx1, profilingEnabled), profilingEnabled);
            }
            setPositivePlan(positivePlan);
        }
    }

    public void initNegativePlan(CommandContext ctx) {
        if (negativePlan == null && negativeStatements != null) {
            if (negativeStatements.size() > 0) {
                BasicCommandContext subCtx2 = new BasicCommandContext();
                subCtx2.setParent(ctx);
                ScriptExecutionPlan negativePlan = new ScriptExecutionPlan(subCtx2);
                for (Statement stm : negativeStatements) {
                    negativePlan.chain(stm.createExecutionPlan(subCtx2, profilingEnabled), profilingEnabled);
                }
                setNegativePlan(negativePlan);
            }
        }
    }


    public BooleanExpression getCondition() {
        return condition;
    }

    public void setCondition(BooleanExpression condition) {
        this.condition = condition;
    }

    public ScriptExecutionPlan getPositivePlan() {
        return positivePlan;
    }

    public void setPositivePlan(ScriptExecutionPlan positivePlan) {
        this.positivePlan = positivePlan;
    }

    public ScriptExecutionPlan getNegativePlan() {
        return negativePlan;
    }

    public void setNegativePlan(ScriptExecutionPlan negativePlan) {
        this.negativePlan = negativePlan;
    }
}
