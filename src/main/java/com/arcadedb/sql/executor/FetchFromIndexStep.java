package com.arcadedb.sql.executor;

import com.arcadedb.database.PDatabase;
import com.arcadedb.database.PIdentifiable;
import com.arcadedb.exception.PCommandExecutionException;
import com.arcadedb.exception.PTimeoutException;
import com.arcadedb.index.PIndex;
import com.arcadedb.index.PIndexCursor;
import com.arcadedb.sql.parser.*;
import com.arcadedb.utility.PPair;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.*;

/**
 * Created by luigidellaquila on 23/07/16.
 */
public class FetchFromIndexStep extends AbstractExecutionStep {
  protected PIndex             index;
  protected OBooleanExpression condition;
  private   OBinaryCondition   additionalRangeCondition;

  private boolean orderAsc;

  protected String indexName;

  private long cost  = 0;
  private long count = 0;

  private boolean inited = false;
  private PIndexCursor cursor;
  private List<PIndexCursor> nextCursors = new ArrayList<>();

//  OMultiCollectionIterator<Map.Entry<Object, PIdentifiable>> customIterator;

  private Iterator nullKeyIterator;
  private PPair<Object, PIdentifiable> nextEntry = null;

  public FetchFromIndexStep(PIndex index, OBooleanExpression condition, OBinaryCondition additionalRangeCondition,
      OCommandContext ctx, boolean profilingEnabled) {
    this(index, condition, additionalRangeCondition, true, ctx, profilingEnabled);
  }

  public FetchFromIndexStep(PIndex index, OBooleanExpression condition, OBinaryCondition additionalRangeCondition, boolean orderAsc,
      OCommandContext ctx, boolean profilingEnabled) {
    super(ctx, profilingEnabled);
    this.index = index;
    this.indexName = index.getName();
    this.condition = condition;
    this.additionalRangeCondition = additionalRangeCondition;
    this.orderAsc = orderAsc;
  }

  public FetchFromIndexStep(String indexName, OBooleanExpression condition, OBinaryCondition additionalRangeCondition,
      boolean orderAsc, OCommandContext ctx, boolean profilingEnabled) {
    super(ctx, profilingEnabled);
    this.indexName = indexName;
    this.condition = condition;
    this.additionalRangeCondition = additionalRangeCondition;
    this.orderAsc = orderAsc;
  }

  @Override
  public OResultSet syncPull(OCommandContext ctx, int nRecords) throws PTimeoutException {
    init(ctx.getDatabase());
    getPrev().ifPresent(x -> x.syncPull(ctx, nRecords));
    return new OResultSet() {
      int localCount = 0;

      @Override
      public boolean hasNext() {
        if (localCount >= nRecords) {
          return false;
        }
        if (nextEntry == null) {
          try {
            fetchNextEntry();
          } catch (IOException e) {
            throw new PCommandExecutionException(e);
          }
        }
        return nextEntry != null;
      }

      @Override
      public OResult next() {
        if (!hasNext()) {
          throw new IllegalStateException();
        }
        long begin = profilingEnabled ? System.nanoTime() : 0;
        try {
          Object key = nextEntry.getFirst();
          PIdentifiable value = nextEntry.getSecond();

          nextEntry = null;

          localCount++;
          OResultInternal result = new OResultInternal();
          result.setProperty("key", key);
          result.setProperty("rid", value);
          ctx.setVariable("$current", result);
          return result;
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
      public Optional<OExecutionPlan> getExecutionPlan() {
        return null;
      }

      @Override
      public Map<String, Long> getQueryStats() {
        return null;
      }
    };
  }

  private void fetchNextEntry() throws IOException {
    nextEntry = null;
    while (true) {
      if (cursor == null) {
        if (nextCursors.size() == 0) {
          if (nextEntry == null && nullKeyIterator != null && nullKeyIterator.hasNext()) {
            PIdentifiable nextValue = (PIdentifiable) nullKeyIterator.next();
            nextEntry = new PPair(null, nextValue);
          } else {
            updateIndexStats();
          }
          return;
        }
        cursor = nextCursors.remove(0);
      }
      if (cursor.hasNext()) {
        cursor.next();
        nextEntry = new PPair(cursor.getKeys(), cursor.getValue());
        count++;
        return;
      }
    }
  }

  private void updateIndexStats() {
    //stats
    OQueryStats stats = OQueryStats.get(ctx.getDatabase());
    if (index == null) {
      return;//this could happen, if not inited yet
    }
    String indexName = index.getName();
    boolean range = false;
    int size = 0;

    if (condition == null) {
      size = 0;
    } else if (condition instanceof OBinaryCondition) {
      size = 1;
    } else if (condition instanceof OBetweenCondition) {
      size = 1;
      range = true;
    } else if (condition instanceof OAndBlock) {
      OAndBlock andBlock = ((OAndBlock) condition);
      size = andBlock.getSubBlocks().size();
      OBooleanExpression lastOp = andBlock.getSubBlocks().get(andBlock.getSubBlocks().size() - 1);
      if (lastOp instanceof OBinaryCondition) {
        OBinaryCompareOperator op = ((OBinaryCondition) lastOp).getOperator();
        range = op.isRangeOperator();
      }
    } else if (condition instanceof OInCondition) {
      size = 1;
    }
    stats.pushIndexStats(indexName, size, range, additionalRangeCondition != null, count);
  }

  private synchronized void init(PDatabase db) {
    if (inited) {
      return;
    }
    inited = true;
    init(condition, db);
  }

  private void init(OBooleanExpression condition, PDatabase db) {
    long begin = profilingEnabled ? System.nanoTime() : 0;
    if (index == null) {
      index = db.getSchema().getIndexByName(indexName);
    }
    try {
      if (condition == null) {
        processFlatIteration();
      } else if (condition instanceof OBinaryCondition) {
        processBinaryCondition();
      } else if (condition instanceof OBetweenCondition) {
        processBetweenCondition();
      } else if (condition instanceof OAndBlock) {
        processAndBlock();
      } else if (condition instanceof OInCondition) {
        processInCondition();
      } else {
        //TODO process containsAny
        throw new PCommandExecutionException("search for index for " + condition + " is not supported yet");
      }
    } catch (IOException e) {
      throw new PCommandExecutionException(e);
    } finally {
      if (profilingEnabled) {
        cost += (System.nanoTime() - begin);
      }
    }
  }

  private void processInCondition() {
    throw new UnsupportedOperationException();
//    OIndexDefinition definition = index.getDefinition();
//    OInCondition inCondition = (OInCondition) condition;
//
//    OExpression left = inCondition.getLeft();
//    if (!left.toString().equalsIgnoreCase("key")) {
//      throw new PCommandExecutionException("search for index for " + condition + " is not supported yet");
//    }
//    Object rightValue = inCondition.evaluateRight((OResult) null, ctx);
//    OEqualsCompareOperator equals = new OEqualsCompareOperator(-1);
//    if (OMultiValue.isMultiValue(rightValue)) {
//      customIterator = new OMultiCollectionIterator<>();
//      for (Object item : OMultiValue.getMultiValueIterable(rightValue)) {
//        PIndexCursor localCursor = createCursor(equals, definition, item, ctx);
//
//        customIterator.add(new Iterator<Map.Entry>() {
//          @Override
//          public boolean hasNext() {
//            return localCursor.hasNext();
//          }
//
//          @Override
//          public Map.Entry next() {
//            if (!localCursor.hasNext()) {
//              throw new IllegalStateException();
//            }
//            PIdentifiable value = localCursor.next();
//            return new Map.Entry() {
//
//              @Override
//              public Object getKey() {
//                return item;
//              }
//
//              @Override
//              public Object getValue() {
//
//                return value;
//              }
//
//              @Override
//              public Object setValue(Object value) {
//                return null;
//              }
//            };
//          }
//        });
//      }
//      customIterator.reset();
//    } else {
//      cursor = createCursor(equals, definition, rightValue, ctx);
//    }
//    fetchNextEntry();
  }

  /**
   * it's not key = [...] but a real condition on field names, already ordered (field names will be ignored)
   */
  private void processAndBlock() throws IOException {
    OCollection fromKey = indexKeyFrom((OAndBlock) condition, additionalRangeCondition);
    OCollection toKey = indexKeyTo((OAndBlock) condition, additionalRangeCondition);
    boolean fromKeyIncluded = indexKeyFromIncluded((OAndBlock) condition, additionalRangeCondition);
    boolean toKeyIncluded = indexKeyToIncluded((OAndBlock) condition, additionalRangeCondition);
    init(fromKey, fromKeyIncluded, toKey, toKeyIncluded);
  }

  private void processFlatIteration() throws IOException {
    cursor = index.iterator(isOrderAsc());

    fetchNullKeys();
    if (cursor != null) {
      fetchNextEntry();
    }
  }

  private void fetchNullKeys() {
//    if (index.getDefinition().isNullValuesIgnored()) {
    nullKeyIterator = Collections.emptyIterator();
//      return;
//    }
//    Object nullIter = index.get(null);
//    if (nullIter instanceof PIdentifiable) {
//      nullKeyIterator = Collections.singleton(nullIter).iterator();
//    } else if (nullIter instanceof Iterable) {
//      nullKeyIterator = ((Iterable) nullIter).iterator();
//    } else if (nullIter instanceof Iterator) {
//      nullKeyIterator = (Iterator) nullIter;
//    } else {
//      nullKeyIterator = Collections.emptyIterator();
//    }
  }

  private void init(OCollection fromKey, boolean fromKeyIncluded, OCollection toKey, boolean toKeyIncluded) throws IOException {
    List<OCollection> secondValueCombinations = cartesianProduct(fromKey);
    List<OCollection> thirdValueCombinations = cartesianProduct(toKey);

    for (int i = 0; i < secondValueCombinations.size(); i++) {

      Object secondValue = secondValueCombinations.get(i).execute((OResult) null, ctx);
      Object thirdValue = thirdValueCombinations.get(i).execute((OResult) null, ctx);

      secondValue = convertToIndexDefinitionTypes(secondValue);
      thirdValue = convertToIndexDefinitionTypes(thirdValue);
      PIndexCursor cursor;
//      if (index.supportsOrderedIterations()) {
      cursor = index.range(toBetweenIndexKey(index, secondValue), toBetweenIndexKey(index, thirdValue));
//      } else if (additionalRangeCondition == null && allEqualities((OAndBlock) condition)) {
//        cursor = index.iterateEntries(toIndexKey(indexDef, secondValue), isOrderAsc());
//      } else {
//        throw new UnsupportedOperationException("Cannot evaluate " + this.condition + " on index " + index);
//      }
      nextCursors.add(cursor);

    }
    if (nextCursors.size() > 0) {
      cursor = nextCursors.remove(0);
      fetchNextEntry();
    }
  }

  private List<OCollection> cartesianProduct(OCollection key) {
    return cartesianProduct(new OCollection(-1), key);//TODO
  }

  private List<OCollection> cartesianProduct(OCollection head, OCollection key) {
    if (key.getExpressions().size() == 0) {
      return Collections.singletonList(head);
    }
    OExpression nextElementInKey = key.getExpressions().get(0);
    Object value = nextElementInKey.execute(new OResultInternal(), ctx);
    if (value instanceof Iterable && !(value instanceof PIdentifiable)) {
      List<OCollection> result = new ArrayList<>();
      for (Object elemInKey : (Collection) value) {
        OCollection newHead = new OCollection(-1);
        for (OExpression exp : head.getExpressions()) {
          newHead.add(exp.copy());
        }
        newHead.add(toExpression(elemInKey, ctx));
        OCollection tail = key.copy();
        tail.getExpressions().remove(0);
        result.addAll(cartesianProduct(newHead, tail));
      }
      return result;
    } else {
      OCollection newHead = new OCollection(-1);
      for (OExpression exp : head.getExpressions()) {
        newHead.add(exp.copy());
      }
      newHead.add(nextElementInKey);
      OCollection tail = key.copy();
      tail.getExpressions().remove(0);
      return cartesianProduct(newHead, tail);
    }

  }

  private OExpression toExpression(Object value, OCommandContext ctx) {
    return new OValueExpression(value);
  }

  private Object convertToIndexDefinitionTypes(Object val/*, OType[] types*/) {
    //TODO
    return val;

//    if (val == null) {
//      return null;
//    }
//    if (OMultiValue.isMultiValue(val)) {
//      List<Object> result = new ArrayList<>();
//      int i = 0;
//      for (Object o : OMultiValue.getMultiValueIterable(val)) {
//        result.add(OType.convert(o, types[i++].getDefaultJavaType()));
//      }
//      return result;
//    }
//    return OType.convert(val, types[0].getDefaultJavaType());
  }

  private boolean allEqualities(OAndBlock condition) {
    if (condition == null) {
      return false;
    }
    for (OBooleanExpression exp : condition.getSubBlocks()) {
      if (exp instanceof OBinaryCondition) {
        if (((OBinaryCondition) exp).getOperator() instanceof OEqualsCompareOperator) {
          return true;
        }
      } else {
        return false;
      }
    }
    return true;
  }

  private void processBetweenCondition() {
    throw new UnsupportedOperationException();
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
//    if (cursor != null) {
//      fetchNextEntry();
//    }
  }

  private void processBinaryCondition() throws IOException {
//    OIndexDefinition definition = index.getDefinition();
    OBinaryCompareOperator operator = ((OBinaryCondition) condition).getOperator();
    OExpression left = ((OBinaryCondition) condition).getLeft();
    if (!left.toString().equalsIgnoreCase("key")) {
      throw new PCommandExecutionException("search for index for " + condition + " is not supported yet");
    }
    Object rightValue = ((OBinaryCondition) condition).getRight().execute((OResult) null, ctx);
    cursor = createCursor(operator, rightValue, ctx);
    if (cursor != null) {
      fetchNextEntry();
    }
  }

//  private Collection toIndexKey(OIndexDefinition definition, Object rightValue) {
//    if (definition.getFields().size() == 1 && rightValue instanceof Collection) {
//      rightValue = ((Collection) rightValue).iterator().next();
//    }
//    if (rightValue instanceof List) {
//      rightValue = definition.createValue((List<?>) rightValue);
//    } else if (!(rightValue instanceof OCompositeKey)) {
//      rightValue = definition.createValue(rightValue);
//    }
//    if (!(rightValue instanceof Collection)) {
//      rightValue = Collections.singleton(rightValue);
//    }
//    return (Collection) rightValue;
//  }

  private Object[] toBetweenIndexKey(PIndex definition, Object rightValue) {
//    if (definition.getFields().size() == 1 && rightValue instanceof Collection) {
//      if (((Collection) rightValue).size() > 0) {
//        rightValue = ((Collection) rightValue).iterator().next();
//      } else {
//        rightValue = null;
//      }
//    }
//    rightValue = definition.createValue(rightValue);
//
//    if (definition.getFields().size() > 1 && !(rightValue instanceof Collection)) {
//      rightValue = Collections.singleton(rightValue);
//    }
//    return rightValue;
    throw new UnsupportedOperationException();
  }

  private PIndexCursor createCursor(OBinaryCompareOperator operator, Object value, OCommandContext ctx)
      throws IOException {
    boolean orderAsc = isOrderAsc();
    if (operator instanceof OEqualsCompareOperator) {
      if (value instanceof Object[]) {
        return index.iterator((Object[]) value);
      } else {
        return index.iterator(new Object[] { value });
      }
    } else if (operator instanceof OGeOperator) {
//      return index.iterateEntriesMajor(value, true, orderAsc);
      throw new UnsupportedEncodingException();
    } else if (operator instanceof OGtOperator) {
//      return index.iterateEntriesMajor(value, false, orderAsc);
      throw new UnsupportedEncodingException();
    } else if (operator instanceof OLeOperator) {
//      return index.iterateEntriesMinor(value, true, orderAsc);
      throw new UnsupportedEncodingException();
    } else if (operator instanceof OLtOperator) {
//      return index.iterateEntriesMinor(value, false, orderAsc);
      throw new UnsupportedEncodingException();
    } else {
      throw new PCommandExecutionException("search for index for " + condition + " is not supported yet");
    }

  }

  protected boolean isOrderAsc() {
    return orderAsc;
  }

  private OCollection indexKeyFrom(OAndBlock keyCondition, OBinaryCondition additional) {
    OCollection result = new OCollection(-1);
    for (OBooleanExpression exp : keyCondition.getSubBlocks()) {
      if (exp instanceof OBinaryCondition) {
        OBinaryCondition binaryCond = ((OBinaryCondition) exp);
        OBinaryCompareOperator operator = binaryCond.getOperator();
        if ((operator instanceof OEqualsCompareOperator) || (operator instanceof OGtOperator)
            || (operator instanceof OGeOperator)) {
          result.add(binaryCond.getRight());
        } else if (additional != null) {
          result.add(additional.getRight());
        }
      } else if (exp instanceof OInCondition) {
        OExpression item = new OExpression(-1);
        if (((OInCondition) exp).getRightMathExpression() != null) {
          item.setMathExpression(((OInCondition) exp).getRightMathExpression());
          result.add(item);
        } else {
          throw new UnsupportedOperationException("Cannot execute index query with " + exp);
        }

      } else if (exp instanceof OContainsAnyCondition) {
        if (((OContainsAnyCondition) exp).getRight() != null) {
          result.add(((OContainsAnyCondition) exp).getRight());
        } else {
          throw new UnsupportedOperationException("Cannot execute index query with " + exp);
        }

      } else {
        throw new UnsupportedOperationException("Cannot execute index query with " + exp);
      }
    }
    return result;
  }

  private OCollection indexKeyTo(OAndBlock keyCondition, OBinaryCondition additional) {
    OCollection result = new OCollection(-1);
    for (OBooleanExpression exp : keyCondition.getSubBlocks()) {
      if (exp instanceof OBinaryCondition) {
        OBinaryCondition binaryCond = ((OBinaryCondition) exp);
        OBinaryCompareOperator operator = binaryCond.getOperator();
        if ((operator instanceof OEqualsCompareOperator) || (operator instanceof OLtOperator)
            || (operator instanceof OLeOperator)) {
          result.add(binaryCond.getRight());
        } else if (additional != null) {
          result.add(additional.getRight());
        }
      } else if (exp instanceof OInCondition) {
        OExpression item = new OExpression(-1);
        if (((OInCondition) exp).getRightMathExpression() != null) {
          item.setMathExpression(((OInCondition) exp).getRightMathExpression());
          result.add(item);
        } else {
          throw new UnsupportedOperationException("Cannot execute index query with " + exp);
        }

      } else if (exp instanceof OContainsAnyCondition) {
        if (((OContainsAnyCondition) exp).getRight() != null) {
          result.add(((OContainsAnyCondition) exp).getRight());
        } else {
          throw new UnsupportedOperationException("Cannot execute index query with " + exp);
        }

      } else {
        throw new UnsupportedOperationException("Cannot execute index query with " + exp);
      }
    }
    return result;
  }

  private boolean indexKeyFromIncluded(OAndBlock keyCondition, OBinaryCondition additional) {
    OBooleanExpression exp = keyCondition.getSubBlocks().get(keyCondition.getSubBlocks().size() - 1);
    OBinaryCompareOperator additionalOperator = additional == null ? null : additional.getOperator();
    if (exp instanceof OBinaryCondition) {
      OBinaryCompareOperator operator = ((OBinaryCondition) exp).getOperator();
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
    } else if (exp instanceof OInCondition || exp instanceof OContainsAnyCondition) {
      if (additional == null || (isIncludeOperator(additionalOperator) && isGreaterOperator(additionalOperator))) {
        return true;
      } else {
        return false;
      }
    } else {
      throw new UnsupportedOperationException("Cannot execute index query with " + exp);
    }
  }

  private boolean isGreaterOperator(OBinaryCompareOperator operator) {
    if (operator == null) {
      return false;
    }
    return operator instanceof OGeOperator || operator instanceof OGtOperator;
  }

  private boolean isLessOperator(OBinaryCompareOperator operator) {
    if (operator == null) {
      return false;
    }
    return operator instanceof OLeOperator || operator instanceof OLtOperator;
  }

  private boolean isIncludeOperator(OBinaryCompareOperator operator) {
    if (operator == null) {
      return false;
    }
    return operator instanceof OGeOperator || operator instanceof OLeOperator;
  }

  private boolean indexKeyToIncluded(OAndBlock keyCondition, OBinaryCondition additional) {
    OBooleanExpression exp = keyCondition.getSubBlocks().get(keyCondition.getSubBlocks().size() - 1);
    OBinaryCompareOperator additionalOperator = additional == null ? null : ((OBinaryCondition) additional).getOperator();
    if (exp instanceof OBinaryCondition) {
      OBinaryCompareOperator operator = ((OBinaryCondition) exp).getOperator();
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
    } else if (exp instanceof OInCondition || exp instanceof OContainsAnyCondition) {
      if (additionalOperator == null || (isIncludeOperator(additionalOperator) && isLessOperator(additionalOperator))) {
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
    String result = OExecutionStepInternal.getIndent(depth, indent) + "+ FETCH FROM INDEX " + indexName;
    if (profilingEnabled) {
      result += " (" + getCostFormatted() + ")";
    }
    if (condition != null) {
      result += ("\n" + OExecutionStepInternal.getIndent(depth, indent) + "  " + condition + (additionalRangeCondition == null ?
          "" :
          " and " + additionalRangeCondition));
    }

    return result;
  }

  @Override
  public long getCost() {
    return cost;
  }

  @Override
  public OResult serialize() {
    OResultInternal result = OExecutionStepInternal.basicSerialize(this);
    result.setProperty("indexName", index.getName());
    if (condition != null) {
      result.setProperty("condition", condition.serialize());
    }
    if (additionalRangeCondition != null) {
      result.setProperty("additionalRangeCondition", additionalRangeCondition.serialize());
    }
    result.setProperty("orderAsc", orderAsc);
    return result;
  }

  @Override
  public void deserialize(OResult fromResult) {
    try {
      OExecutionStepInternal.basicDeserialize(fromResult, this);
      indexName = fromResult.getProperty("indexName");
      if (fromResult.getProperty("condition") != null) {
        condition = OBooleanExpression.deserializeFromOResult(fromResult.getProperty("condition"));
      }
      if (fromResult.getProperty("additionalRangeCondition") != null) {
        additionalRangeCondition = new OBinaryCondition(-1);
        additionalRangeCondition.deserialize(fromResult.getProperty("additionalRangeCondition"));
      }
      orderAsc = fromResult.getProperty("orderAsc");
    } catch (Exception e) {
      throw new PCommandExecutionException(e);
    }
  }

  @Override
  public void reset() {
    index = null;
    condition = condition == null ? null : condition.copy();
    additionalRangeCondition = additionalRangeCondition == null ? null : additionalRangeCondition.copy();

    cost = 0;
    count = 0;

    inited = false;
    cursor = null;
//    customIterator = null;
    nullKeyIterator = null;
    nextEntry = null;
  }

  @Override
  public boolean canBeCached() {
    return true;
  }

  @Override
  public OExecutionStep copy(OCommandContext ctx) {
    FetchFromIndexStep result = new FetchFromIndexStep(indexName, this.condition == null ? null : this.condition.copy(),
        this.additionalRangeCondition == null ? null : this.additionalRangeCondition.copy(), this.orderAsc, ctx,
        this.profilingEnabled);
    return result;
  }
}
