/*
 * Copyright (c) - Arcade Analytics LTD (https://arcadeanalytics.com)
 */

package com.arcadedb.sql.executor;

import com.arcadedb.exception.TimeoutException;
import com.arcadedb.sql.parser.IfStatement;
import com.arcadedb.sql.parser.ReturnStatement;
import com.arcadedb.sql.parser.Statement;

/**
 * @author Luigi Dell'Aquila (l.dellaquila-(at)-orientdb.com)
 * <p>
 * This step represents the execution plan of an instruciton instide a batch script
 */
public class ScriptLineStep extends AbstractExecutionStep {
    private final InternalExecutionPlan plan;

    boolean executed = false;

    public ScriptLineStep(InternalExecutionPlan nextPlan, CommandContext ctx, boolean profilingEnabled) {
        super(ctx, profilingEnabled);
        this.plan = nextPlan;
    }

    @Override
    public ResultSet syncPull(CommandContext ctx, int nRecords) throws TimeoutException {
        if (!executed) {
            if (plan instanceof InsertExecutionPlan) {
                ((InsertExecutionPlan) plan).executeInternal();
            } else if (plan instanceof DeleteExecutionPlan) {
                ((DeleteExecutionPlan) plan).executeInternal();
            } else if (plan instanceof UpdateExecutionPlan) {
                ((UpdateExecutionPlan) plan).executeInternal();
            } else if (plan instanceof ODDLExecutionPlan) {
                ((ODDLExecutionPlan) plan).executeInternal((BasicCommandContext) ctx);
            } else if (plan instanceof SingleOpExecutionPlan) {
                ((SingleOpExecutionPlan) plan).executeInternal((BasicCommandContext) ctx);
            }
            executed = true;
        }
        return plan.fetchNext(nRecords);
    }

    public boolean containsReturn() {
        if (plan instanceof ScriptExecutionPlan) {
            return ((ScriptExecutionPlan) plan).containsReturn();
        }
        if (plan instanceof SingleOpExecutionPlan) {
            if (((SingleOpExecutionPlan) plan).statement instanceof ReturnStatement) {
                return true;
            }
        }
        if (plan instanceof IfExecutionPlan) {
            IfStep step = (IfStep) plan.getSteps().get(0);
            if (step.positivePlan != null && step.positivePlan.containsReturn()) {
                return true;
            } else if (step.positiveStatements != null) {
                for (Statement stm : step.positiveStatements) {
                    if (containsReturn(stm)) {
                        return true;
                    }
                }
            }
        }

        if (plan instanceof ForEachExecutionPlan) {
            if (((ForEachExecutionPlan) plan).containsReturn()) {
                return true;
            }
        }
        return false;
    }

    private boolean containsReturn(Statement stm) {
        if (stm instanceof ReturnStatement) {
            return true;
        }
        if (stm instanceof IfStatement) {
            for (Statement o : ((IfStatement) stm).getStatements()) {
                if (containsReturn(o)) {
                    return true;
                }
            }
        }
        return false;
    }

    public ExecutionStepInternal executeUntilReturn(CommandContext ctx) {
        if (plan instanceof ScriptExecutionPlan) {
            return ((ScriptExecutionPlan) plan).executeUntilReturn();
        }
        if (plan instanceof SingleOpExecutionPlan) {
            if (((SingleOpExecutionPlan) plan).statement instanceof ReturnStatement) {
                return new ReturnStep(((SingleOpExecutionPlan) plan).statement, ctx, profilingEnabled);
            }
        }
        if (plan instanceof IfExecutionPlan) {
            return ((IfExecutionPlan) plan).executeUntilReturn();
        }
        throw new IllegalStateException();
    }
}
