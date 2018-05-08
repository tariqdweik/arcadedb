package com.arcadedb.sql.executor;

import com.arcadedb.database.Identifiable;
import com.arcadedb.exception.CommandExecutionException;
import com.arcadedb.exception.TimeoutException;
import com.arcadedb.index.PIndex;
import com.arcadedb.index.PIndexCursor;
import com.arcadedb.sql.parser.*;
import com.arcadedb.utility.Pair;

import java.io.IOException;
import java.util.Map;
import java.util.Optional;

/**
 * Created by luigidellaquila on 11/08/16.
 */
public class DeleteFromIndexStep extends AbstractExecutionStep {
  protected final PIndex            index;
  private final   BinaryCondition   additional;
  private final   BooleanExpression ridCondition;
  private final   boolean           orderAsc;

  Pair<Object, Identifiable> nextEntry = null;

  BooleanExpression condition;

  private boolean inited = false;
  private PIndexCursor cursor;

  private long cost = 0;

  public DeleteFromIndexStep(PIndex index, BooleanExpression condition, BinaryCondition additionalRangeCondition,
      BooleanExpression ridCondition, CommandContext ctx, boolean profilingEnabled) {
    this(index, condition, additionalRangeCondition, ridCondition, true, ctx, profilingEnabled);
  }

  public DeleteFromIndexStep(PIndex index, BooleanExpression condition, BinaryCondition additionalRangeCondition,
      BooleanExpression ridCondition, boolean orderAsc, CommandContext ctx, boolean profilingEnabled) {
    super(ctx, profilingEnabled);
    this.index = index;
    this.condition = condition;
    this.additional = additionalRangeCondition;
    this.ridCondition = ridCondition;
    this.orderAsc = orderAsc;
  }

  @Override
  public ResultSet syncPull(CommandContext ctx, int nRecords) throws TimeoutException {
    getPrev().ifPresent(x -> x.syncPull(ctx, nRecords));
    init();

    return new ResultSet() {
      int localCount = 0;

      @Override
      public boolean hasNext() {
        return (localCount < nRecords && nextEntry != null);
      }

      @Override
      public Result next() {
        long begin = profilingEnabled ? System.nanoTime() : 0;
        try {
          if (!hasNext()) {
            throw new IllegalStateException();
          }
          Pair<Object, Identifiable> entry = nextEntry;
          ResultInternal result = new ResultInternal();
          Identifiable value = entry.getSecond();

          throw new UnsupportedOperationException("index remove");
//          index.remove(entry.getKey(), value);
//          localCount++;
//          nextEntry = loadNextEntry(ctx);
//          return result;
        } finally {
          if (profilingEnabled) {
            cost += (System.nanoTime() - begin);
          }
        }
      }

      @Override
      public void close() {
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

  private synchronized void init() {
    if (inited) {
      return;
    }
    inited = true;
    long begin = profilingEnabled ? System.nanoTime() : 0;
    try {
      init(condition);
      nextEntry = loadNextEntry(ctx);
    } catch (IOException e) {
      e.printStackTrace();
    } finally {
      if (profilingEnabled) {
        cost += (System.nanoTime() - begin);
      }
    }
  }

  private Pair<Object, Identifiable> loadNextEntry(CommandContext commandContext) throws IOException {
    while (cursor.hasNext()) {
      cursor.next();

      Pair<Object, Identifiable> result = new Pair(cursor.getKeys(), cursor.getValue());
      if (ridCondition == null) {
        return result;
      }
      ResultInternal res = new ResultInternal();
      res.setProperty("rid", result.getSecond());
      if (ridCondition.evaluate(res, commandContext)) {
        return result;
      }
    }
    return null;
  }

  private void init(BooleanExpression condition) throws IOException {
    if (condition == null) {
      processFlatIteration();
    } else if (condition instanceof BinaryCondition) {
      processBinaryCondition();
    } else if (condition instanceof BetweenCondition) {
      processBetweenCondition();
    } else if (condition instanceof AndBlock) {
      processAndBlock();
    } else {
      throw new CommandExecutionException("search for index for " + condition + " is not supported yet");
    }
  }

  /**
   * it's not key = [...] but a real condition on field names, already ordered (field names will be ignored)
   */
  private void processAndBlock() {
    PCollection fromKey = indexKeyFrom((AndBlock) condition, additional);
    PCollection toKey = indexKeyTo((AndBlock) condition, additional);
    boolean fromKeyIncluded = indexKeyFromIncluded((AndBlock) condition, additional);
    boolean toKeyIncluded = indexKeyToIncluded((AndBlock) condition, additional);
    init(fromKey, fromKeyIncluded, toKey, toKeyIncluded);
  }

  private void processFlatIteration() throws IOException {
    cursor = index.iterator(isOrderAsc());
  }

  private void init(PCollection fromKey, boolean fromKeyIncluded, PCollection toKey, boolean toKeyIncluded) {
    Object secondValue = fromKey.execute((Result) null, ctx);
    Object thirdValue = toKey.execute((Result) null, ctx);
//    OIndexDefinition indexDef = index.getDefinition();
//    if (index.supportsOrderedIterations()) {
//      cursor = index
//          .iterateEntriesBetween(toBetweenIndexKey(indexDef, secondValue), fromKeyIncluded, toBetweenIndexKey(indexDef, thirdValue),
//              toKeyIncluded, isOrderAsc());
//    } else if (additional == null && allEqualities((OAndBlock) condition)) {
//      cursor = index.iterateEntries(toIndexKey(indexDef, secondValue), isOrderAsc());
//    } else {
//      throw new UnsupportedOperationException("Cannot evaluate " + this.condition + " on index " + index);
//    }

    throw new UnsupportedOperationException();
  }

  private boolean allEqualities(AndBlock condition) {
    if (condition == null) {
      return false;
    }
    for (BooleanExpression exp : condition.getSubBlocks()) {
      if (exp instanceof BinaryCondition) {
        if (((BinaryCondition) exp).getOperator() instanceof EqualsCompareOperator) {
          return true;
        }
      } else {
        return false;
      }
    }
    return true;
  }

  private void processBetweenCondition() {
//    OIndexDefinition definition = index.getDefinition();
//    OExpression key = ((OBetweenCondition) condition).getFirst();
//    if (!key.toString().equalsIgnoreCase("key")) {
//      throw new PCommandExecutionException("search for index for " + condition + " is not supported yet");
//    }
//    OExpression second = ((OBetweenCondition) condition).getSecond();
//    OExpression third = ((OBetweenCondition) condition).getThird();
//
//    Object secondValue = second.execute((OResult) null, ctx);
//    Object thirdValue = third.execute((OResult) null, ctx);
//    cursor = index
//        .iterateEntriesBetween(toBetweenIndexKey(definition, secondValue), true, toBetweenIndexKey(definition, thirdValue), true,
//            isOrderAsc());
    throw new UnsupportedOperationException();
  }

  private void processBinaryCondition() {
//    OIndexDefinition definition = index.getDefinition();
//    OBinaryCompareOperator operator = ((OBinaryCondition) condition).getOperator();
//    OExpression left = ((OBinaryCondition) condition).getLeft();
//    if (!left.toString().equalsIgnoreCase("key")) {
//      throw new PCommandExecutionException("search for index for " + condition + " is not supported yet");
//    }
//    Object rightValue = ((OBinaryCondition) condition).getRight().execute((OResult) null, ctx);
//    cursor = createCursor(operator, definition, rightValue, ctx);
    throw new UnsupportedOperationException();
  }

//  private Collection toIndexKey(OIndexDefinition definition, Object rightValue) {
//    if (definition.getFields().size() == 1 && rightValue instanceof Collection) {
//      rightValue = ((Collection) rightValue).iterator().next();
//    }
//    if (rightValue instanceof List) {
//      rightValue = definition.createValue((List<?>) rightValue);
//    } else {
//      rightValue = definition.createValue(rightValue);
//    }
//    if (!(rightValue instanceof Collection)) {
//      rightValue = Collections.singleton(rightValue);
//    }
//    return (Collection) rightValue;
//  }
//
//  private Object toBetweenIndexKey(OIndexDefinition definition, Object rightValue) {
//    if (definition.getFields().size() == 1 && rightValue instanceof Collection) {
//      rightValue = ((Collection) rightValue).iterator().next();
//    }
//    rightValue = definition.createValue(rightValue);
//
//    if (definition.getFields().size() > 1 && !(rightValue instanceof Collection)) {
//      rightValue = Collections.singleton(rightValue);
//    }
//    return rightValue;
//  }
//
//  private OIndexCursor createCursor(OBinaryCompareOperator operator, OIndexDefinition definition, Object value,
//      OCommandContext ctx) {
//    boolean orderAsc = isOrderAsc();
//    if (operator instanceof OEqualsCompareOperator) {
//      return index.iterateEntries(toIndexKey(definition, value), orderAsc);
//    } else if (operator instanceof OGeOperator) {
//      return index.iterateEntriesMajor(value, true, orderAsc);
//    } else if (operator instanceof OGtOperator) {
//      return index.iterateEntriesMajor(value, false, orderAsc);
//    } else if (operator instanceof OLeOperator) {
//      return index.iterateEntriesMinor(value, true, orderAsc);
//    } else if (operator instanceof OLtOperator) {
//      return index.iterateEntriesMinor(value, false, orderAsc);
//    } else {
//      throw new PCommandExecutionException("search for index for " + condition + " is not supported yet");
//    }
//
//  }

  protected boolean isOrderAsc() {
    return orderAsc;
  }

  private PCollection indexKeyFrom(AndBlock keyCondition, BinaryCondition additional) {
    PCollection result = new PCollection(-1);
    for (BooleanExpression exp : keyCondition.getSubBlocks()) {
      if (exp instanceof BinaryCondition) {
        BinaryCondition binaryCond = ((BinaryCondition) exp);
        BinaryCompareOperator operator = binaryCond.getOperator();
        if ((operator instanceof EqualsCompareOperator) || (operator instanceof GtOperator)
            || (operator instanceof GeOperator)) {
          result.add(binaryCond.getRight());
        } else if (additional != null) {
          result.add(additional.getRight());
        }
      } else {
        throw new UnsupportedOperationException("Cannot execute index query with " + exp);
      }
    }
    return result;
  }

  private PCollection indexKeyTo(AndBlock keyCondition, BinaryCondition additional) {
    PCollection result = new PCollection(-1);
    for (BooleanExpression exp : keyCondition.getSubBlocks()) {
      if (exp instanceof BinaryCondition) {
        BinaryCondition binaryCond = ((BinaryCondition) exp);
        BinaryCompareOperator operator = binaryCond.getOperator();
        if ((operator instanceof EqualsCompareOperator) || (operator instanceof LtOperator)
            || (operator instanceof LeOperator)) {
          result.add(binaryCond.getRight());
        } else if (additional != null) {
          result.add(additional.getRight());
        }
      } else {
        throw new UnsupportedOperationException("Cannot execute index query with " + exp);
      }
    }
    return result;
  }

  private boolean indexKeyFromIncluded(AndBlock keyCondition, BinaryCondition additional) {
    BooleanExpression exp = keyCondition.getSubBlocks().get(keyCondition.getSubBlocks().size() - 1);
    if (exp instanceof BinaryCondition) {
      BinaryCompareOperator operator = ((BinaryCondition) exp).getOperator();
      BinaryCompareOperator additionalOperator = additional == null ? null : ((BinaryCondition) additional).getOperator();
      if (isGreaterOperator(operator)) {
        if (isIncludeOperator(operator)) {
          return true;
        } else {
          return false;
        }
      } else if (additionalOperator == null || (isIncludeOperator(additionalOperator) && isGreaterOperator(additionalOperator))) {
        return true;
      } else {
        return false;
      }
    } else {
      throw new UnsupportedOperationException("Cannot execute index query with " + exp);
    }
  }

  private boolean isGreaterOperator(BinaryCompareOperator operator) {
    if (operator == null) {
      return false;
    }
    return operator instanceof GeOperator || operator instanceof GtOperator;
  }

  private boolean isLessOperator(BinaryCompareOperator operator) {
    if (operator == null) {
      return false;
    }
    return operator instanceof LeOperator || operator instanceof LtOperator;
  }

  private boolean isIncludeOperator(BinaryCompareOperator operator) {
    if (operator == null) {
      return false;
    }
    return operator instanceof GeOperator || operator instanceof LeOperator;
  }

  private boolean indexKeyToIncluded(AndBlock keyCondition, BinaryCondition additional) {
    BooleanExpression exp = keyCondition.getSubBlocks().get(keyCondition.getSubBlocks().size() - 1);
    if (exp instanceof BinaryCondition) {
      BinaryCompareOperator operator = ((BinaryCondition) exp).getOperator();
      BinaryCompareOperator additionalOperator = additional == null ? null : ((BinaryCondition) additional).getOperator();
      if (isLessOperator(operator)) {
        if (isIncludeOperator(operator)) {
          return true;
        } else {
          return false;
        }
      } else if (additionalOperator == null || (isIncludeOperator(additionalOperator) && isLessOperator(additionalOperator))) {
        return true;
      } else {
        return false;
      }
    } else {
      throw new UnsupportedOperationException("Cannot execute index query with " + exp);
    }
  }

  @Override
  public String prettyPrint(int depth, int indent) {
    String result = ExecutionStepInternal.getIndent(depth, indent) + "+ DELETE FROM INDEX " + index.getName();
    if (profilingEnabled) {
      result += " (" + getCostFormatted() + ")";
    }
    result += (condition == null ?
        "" :
        ("\n" + ExecutionStepInternal.getIndent(depth, indent) + "  " + condition + (additional == null ?
            "" :
            " and " + additional)));
    return result;
  }

  @Override
  public long getCost() {
    return cost;
  }
}
