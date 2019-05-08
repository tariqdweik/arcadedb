package com.arcadedb.sql.executor;

/**
 * Created by luigidellaquila on 08/08/16.
 */


import com.arcadedb.sql.parser.WhileStep;

/**
 * @author Luigi Dell'Aquila (l.dellaquila-(at)-orientdb.com)
 */
public class ForEachExecutionPlan extends UpdateExecutionPlan {
    public ForEachExecutionPlan(CommandContext ctx) {
        super(ctx);
    }

    public boolean containsReturn() {
        for (ExecutionStep step : getSteps()) {
            if (step instanceof ForEachStep) {
                return ((ForEachStep) step).containsReturn();
            }
            if (step instanceof WhileStep) {
                return ((WhileStep) step).containsReturn();
            }
        }

        return false;
    }

}

