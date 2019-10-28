/*
 * Copyright (c) - Arcade Analytics LTD (https://arcadeanalytics.com)
 */

package com.arcadedb.sql.executor;

import com.arcadedb.exception.TimeoutException;

import java.util.Map;
import java.util.Optional;

/**
 * <p>This is intended for INSERT FROM SELECT. This step removes existing edge pointers so that the resulting graph is still
 * consistent </p>
 *
 * @author Luigi Dell'Aquila (l.dellaquila-(at)-orientdb.com)
 */
public class RemoveEdgePointersStep extends AbstractExecutionStep {

  private long cost = 0;

  public RemoveEdgePointersStep(CommandContext ctx, boolean profilingEnabled) {
    super(ctx, profilingEnabled);
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
//        OResultInternal elem = (OResultInternal) upstream.next();
//        long begin = profilingEnabled ? System.nanoTime() : 0;
//        try {
//
//          Set<String> propNames = elem.getPropertyNames();
//          for (String propName : propNames.stream().filter(x -> x.startsWith("in_") || x.startsWith("out_"))
//              .collect(Collectors.toList())) {
//            Object val = elem.getProperty(propName);
//            if (val instanceof PRecord) {
//              if (((OElement) val).getSchemaType().map(x -> x.isSubClassOf("E")).orElse(false)) {
//                elem.removeProperty(propName);
//              }
//            } else if (val instanceof Iterable) {
//              for (Object o : (Iterable) val) {
//                if (o instanceof OElement) {
//                  if (((OElement) o).getSchemaType().map(x -> x.isSubClassOf("E")).orElse(false)) {
//                    elem.removeProperty(propName);
//                    break;
//                  }
//                }
//              }
//            }
//          }
//        } finally {
//          if (profilingEnabled) {
//            cost += (System.nanoTime() - begin);
//          }
//        }
//        return elem;
        throw new UnsupportedOperationException();//TODO
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
    result.append("+ CHECK AND EXCLUDE (possible) EXISTING EDGES ");
    if (profilingEnabled) {
      result.append(" (" + getCostFormatted() + ")");
    }
    return result.toString();
  }

  @Override
  public long getCost() {
    return cost;
  }
}
