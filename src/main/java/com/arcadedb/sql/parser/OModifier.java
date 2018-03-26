/* Generated By:JJTree: Do not edit this line. OModifier.java Version 4.3 */
/* JavaCCOptions:MULTI=true,NODE_USES_PARSER=false,VISITOR=true,TRACK_TOKENS=true,NODE_PREFIX=O,NODE_EXTENDS=,NODE_FACTORY=,SUPPORT_CLASS_VISIBILITY_PUBLIC=true */
package com.arcadedb.sql.parser;

import com.arcadedb.database.PIdentifiable;
import com.arcadedb.exception.PCommandExecutionException;
import com.arcadedb.sql.executor.OCommandContext;
import com.arcadedb.sql.executor.OMultiValue;
import com.arcadedb.sql.executor.OResult;
import com.arcadedb.sql.executor.OResultInternal;

import java.lang.reflect.Array;
import java.util.*;

public class OModifier extends SimpleNode {

  boolean squareBrackets = false;
  OArrayRangeSelector        arrayRange;
  OOrBlock                   condition;
  OArraySingleValuesSelector arraySingleValues;
  ORightBinaryCondition      rightBinaryCondition;
  OMethodCall                methodCall;
  OSuffixIdentifier          suffix;

  OModifier next;

  public OModifier(int id) {
    super(id);
  }

  public OModifier(OrientSql p, int id) {
    super(p, id);
  }

  /**
   * Accept the visitor.
   **/
  public Object jjtAccept(OrientSqlVisitor visitor, Object data) {
    return visitor.visit(this, data);
  }

  public void toString(Map<Object, Object> params, StringBuilder builder) {

    if (squareBrackets) {
      builder.append("[");

      if (arrayRange != null) {
        arrayRange.toString(params, builder);
      } else if (condition != null) {
        condition.toString(params, builder);
      } else if (arraySingleValues != null) {
        arraySingleValues.toString(params, builder);
      } else if (rightBinaryCondition != null) {
        rightBinaryCondition.toString(params, builder);
      }

      builder.append("]");
    } else if (methodCall != null) {
      methodCall.toString(params, builder);
    } else if (suffix != null) {
      builder.append(".");
      suffix.toString(params, builder);
    }
    if (next != null) {
      next.toString(params, builder);
    }
  }

  public Object execute(PIdentifiable iCurrentRecord, Object result, OCommandContext ctx) {
    if (methodCall != null) {
      result = methodCall.execute(result, ctx);
    } else if (suffix != null) {
      result = suffix.execute(result, ctx);
    } else if (arrayRange != null) {
      result = arrayRange.execute(iCurrentRecord, result, ctx);
    } else if (condition != null) {
      result = filterByCondition(result, ctx);
    } else if (arraySingleValues != null) {
      result = arraySingleValues.execute(iCurrentRecord, result, ctx);
    } else if (rightBinaryCondition != null) {
      result = rightBinaryCondition.execute(iCurrentRecord, result, ctx);
    }
    if (next != null) {
      result = next.execute(iCurrentRecord, result, ctx);
    }
    return result;
  }

  public Object execute(OResult iCurrentRecord, Object result, OCommandContext ctx) {
    if (methodCall != null) {
      result = methodCall.execute(result, ctx);
    } else if (suffix != null) {
      result = suffix.execute(result, ctx);
    } else if (arrayRange != null) {
      result = arrayRange.execute(iCurrentRecord, result, ctx);
    } else if (condition != null) {
      result = filterByCondition(result, ctx);
    } else if (arraySingleValues != null) {
      result = arraySingleValues.execute(iCurrentRecord, result, ctx);
    } else if (rightBinaryCondition != null) {
      result = rightBinaryCondition.execute(iCurrentRecord, result, ctx);
    }
    if (next != null) {
      result = next.execute(iCurrentRecord, result, ctx);
    }
    return result;
  }

  private Object filterByCondition(Object iResult, OCommandContext ctx) {
    if (iResult == null) {
      return null;
    }
    List<Object> result = new ArrayList<Object>();
    if (iResult.getClass().isArray()) {
      for (int i = 0; i < Array.getLength(iResult); i++) {
        Object item = Array.get(iResult, i);
        if (condition.evaluate(item, ctx)) {
          result.add(item);
        }
      }
      return result;
    }
    if (iResult instanceof PIdentifiable) {
      iResult = Collections.singleton(iResult);
    }
    if (iResult instanceof Iterable) {
      iResult = ((Iterable) iResult).iterator();
    }
    if (iResult instanceof Iterator) {
      while (((Iterator) iResult).hasNext()) {
        Object item = ((Iterator) iResult).next();
        if (condition.evaluate(item, ctx)) {
          result.add(item);
        }
      }
    }
    return result;
  }

  public boolean needsAliases(Set<String> aliases) {
    if (condition != null && condition.needsAliases(aliases)) {
      return true;
    }

    if (arraySingleValues != null && arraySingleValues.needsAliases(aliases)) {
      return true;
    }

    if (arrayRange != null && arrayRange.needsAliases(aliases)) {
      return true;
    }

    if (rightBinaryCondition != null && rightBinaryCondition.needsAliases(aliases)) {
      return true;
    }

    if (methodCall != null && methodCall.needsAliases(aliases)) {
      return true;
    }

    if (next != null && next.needsAliases(aliases)) {
      return true;
    }

    return false;
  }

  public OModifier copy() {
    OModifier result = new OModifier(-1);
    result.squareBrackets = squareBrackets;
    result.arrayRange = arrayRange == null ? null : arrayRange.copy();
    result.condition = condition == null ? null : condition.copy();
    result.arraySingleValues = arraySingleValues == null ? null : arraySingleValues.copy();
    result.rightBinaryCondition = rightBinaryCondition == null ? null : rightBinaryCondition.copy();
    result.methodCall = methodCall == null ? null : methodCall.copy();
    result.suffix = suffix == null ? null : suffix.copy();
    result.next = next == null ? null : next.copy();

    return result;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o)
      return true;
    if (o == null || getClass() != o.getClass())
      return false;

    OModifier oModifier = (OModifier) o;

    if (squareBrackets != oModifier.squareBrackets)
      return false;
    if (arrayRange != null ? !arrayRange.equals(oModifier.arrayRange) : oModifier.arrayRange != null)
      return false;
    if (condition != null ? !condition.equals(oModifier.condition) : oModifier.condition != null)
      return false;
    if (arraySingleValues != null ? !arraySingleValues.equals(oModifier.arraySingleValues) : oModifier.arraySingleValues != null)
      return false;
    if (rightBinaryCondition != null ?
        !rightBinaryCondition.equals(oModifier.rightBinaryCondition) :
        oModifier.rightBinaryCondition != null)
      return false;
    if (methodCall != null ? !methodCall.equals(oModifier.methodCall) : oModifier.methodCall != null)
      return false;
    if (suffix != null ? !suffix.equals(oModifier.suffix) : oModifier.suffix != null)
      return false;
    if (next != null ? !next.equals(oModifier.next) : oModifier.next != null)
      return false;

    return true;
  }

  @Override
  public int hashCode() {
    int result = (squareBrackets ? 1 : 0);
    result = 31 * result + (arrayRange != null ? arrayRange.hashCode() : 0);
    result = 31 * result + (condition != null ? condition.hashCode() : 0);
    result = 31 * result + (arraySingleValues != null ? arraySingleValues.hashCode() : 0);
    result = 31 * result + (rightBinaryCondition != null ? rightBinaryCondition.hashCode() : 0);
    result = 31 * result + (methodCall != null ? methodCall.hashCode() : 0);
    result = 31 * result + (suffix != null ? suffix.hashCode() : 0);
    result = 31 * result + (next != null ? next.hashCode() : 0);
    return result;
  }

  public void extractSubQueries(SubQueryCollector collector) {
    if (arrayRange != null) {
      arrayRange.extractSubQueries(collector);
    }
    if (condition != null) {
      condition.extractSubQueries(collector);
    }
    if (arraySingleValues != null) {
      arraySingleValues.extractSubQueries(collector);
    }
    if (rightBinaryCondition != null) {
      rightBinaryCondition.extractSubQueries(collector);
    }
    if (methodCall != null) {
      methodCall.extractSubQueries(collector);
    }
    if (suffix != null) {
      suffix.extractSubQueries(collector);
    }
    if (next != null) {
      next.extractSubQueries(collector);
    }

  }

  public boolean refersToParent() {
    if (arrayRange != null && arrayRange.refersToParent()) {
      return true;
    }
    if (condition != null && condition.refersToParent()) {
      return true;
    }

    if (arraySingleValues != null && arraySingleValues.refersToParent()) {
      return true;
    }
    if (rightBinaryCondition != null && rightBinaryCondition.refersToParent()) {
      return true;
    }
    if (methodCall != null && methodCall.refersToParent()) {
      return true;
    }
    if (suffix != null && suffix.refersToParent()) {
      return true;
    }
    return false;
  }

  protected void setValue(OResult currentRecord, Object target, Object value, OCommandContext ctx) {
    if (next == null) {
      doSetValue(currentRecord, target, value, ctx);
    } else {
      Object newTarget = calculateLocal(currentRecord, target, ctx);
      if (newTarget != null) {
        next.setValue(currentRecord, newTarget, value, ctx);
      }
    }
  }

  private void doSetValue(OResult currentRecord, Object target, Object value, OCommandContext ctx) {
    if (methodCall != null) {
      //do nothing
    } else if (suffix != null) {
      suffix.setValue(target, value, ctx);
    } else if (arrayRange != null) {
      arrayRange.setValue(target, value, ctx);
    } else if (condition != null) {
      //TODO
      throw new UnsupportedOperationException("SET value on conditional filtering will be supported soon");
    } else if (arraySingleValues != null) {
      arraySingleValues.setValue(currentRecord, target, value, ctx);
    } else if (rightBinaryCondition != null) {
      throw new UnsupportedOperationException("SET value on conditional filtering will be supported soon");
    }
  }

  private Object calculateLocal(OResult currentRecord, Object target, OCommandContext ctx) {
    if (methodCall != null) {
      return methodCall.execute(target, ctx);
    } else if (suffix != null) {
      return suffix.execute(target, ctx);
    } else if (arrayRange != null) {
      return arrayRange.execute(currentRecord, target, ctx);
    } else if (condition != null) {
      if (target instanceof OResult || target instanceof PIdentifiable || target instanceof Map) {
        if (condition.evaluate(target, ctx)) {
          return target;
        } else {
          return null;
        }
      } else if (OMultiValue.isMultiValue(target)) {
        List<Object> result = new ArrayList<>();
        for (Object o : OMultiValue.getMultiValueIterable(target)) {
          if (condition.evaluate(target, ctx)) {
            result.add(o);
          }
        }
        return result;
      } else {
        return null;
      }
    } else if (arraySingleValues != null) {
      return arraySingleValues.execute(currentRecord, target, ctx);
    } else if (rightBinaryCondition != null) {
      return rightBinaryCondition.execute(currentRecord, target, ctx);
    }
    return null;

  }

  public void applyRemove(Object currentValue, OResultInternal originalRecord, OCommandContext ctx) {
    if (next != null) {
      Object val = calculateLocal(originalRecord, currentValue, ctx);
      next.applyRemove(val, originalRecord, ctx);
    } else {
      if (arrayRange != null) {
        arrayRange.applyRemove(currentValue, originalRecord, ctx);
      } else if (condition != null) {
//TODO
        throw new UnsupportedOperationException("Remove on conditional filtering will be supported soon");
      } else if (arraySingleValues != null) {
        arraySingleValues.applyRemove(currentValue, originalRecord, ctx);
      } else if (rightBinaryCondition != null) {
        throw new UnsupportedOperationException("Remove on conditional filtering will be supported soon");
      } else if (suffix != null) {
        suffix.applyRemove(currentValue, ctx);
      } else {
        throw new PCommandExecutionException("cannot apply REMOVE " + toString());
      }
    }

  }

  public OResult serialize() {
    OResultInternal result = new OResultInternal();
    result.setProperty("squareBrackets", squareBrackets);
    if (arrayRange != null) {
      result.setProperty("arrayRange", arrayRange.serialize());
    }
    if (condition != null) {
      result.setProperty("condition", condition.serialize());
    }
    if (arraySingleValues != null) {
      result.setProperty("arraySingleValues", arraySingleValues.serialize());
    }
    if (rightBinaryCondition != null) {
      result.setProperty("rightBinaryCondition", rightBinaryCondition.serialize());
    }
    if (methodCall != null) {
      result.setProperty("methodCall", methodCall.serialize());
    }
    if (suffix != null) {
      result.setProperty("suffix", suffix.serialize());
    }
    if (next != null) {
      result.setProperty("next", next.serialize());
    }
    return result;
  }

  public void deserialize(OResult fromResult) {
    squareBrackets = fromResult.getProperty("squareBrackets");

    if (fromResult.getProperty("arrayRange") != null) {
      arrayRange = new OArrayRangeSelector(-1);
      arrayRange.deserialize(fromResult.getProperty("arrayRange"));
    }
    if (fromResult.getProperty("condition") != null) {
      condition = new OOrBlock(-1);
      condition.deserialize(fromResult.getProperty("condition"));
    }
    if (fromResult.getProperty("arraySingleValues") != null) {
      arraySingleValues = new OArraySingleValuesSelector(-1);
      arraySingleValues.deserialize(fromResult.getProperty("arraySingleValues"));
    }
    if (fromResult.getProperty("rightBinaryCondition") != null) {
      rightBinaryCondition = new ORightBinaryCondition(-1);
      rightBinaryCondition.deserialize(fromResult.getProperty("arraySingleValues"));
    }
    if (fromResult.getProperty("methodCall") != null) {
      methodCall = new OMethodCall(-1);
      methodCall.deserialize(fromResult.getProperty("methodCall"));
    }
    if (fromResult.getProperty("suffix") != null) {
      suffix = new OSuffixIdentifier(-1);
      suffix.deserialize(fromResult.getProperty("suffix"));
    }

    if (fromResult.getProperty("next") != null) {
      next = new OModifier(-1);
      next.deserialize(fromResult.getProperty("next"));
    }
  }

  public boolean isCacheable() {
    if (arrayRange != null || arraySingleValues != null || rightBinaryCondition != null) {
      return false;//TODO enhance a bit
    }
    if (condition != null && !condition.isCacheable()) {
      return false;
    }
    if (methodCall != null && !methodCall.isCacheable()) {
      return false;
    }
    if (suffix != null && !suffix.isCacheable()) {
      return false;
    }
    if (next != null && !next.isCacheable()) {
      return false;
    }

    return true;

  }
}
/* JavaCC - OriginalChecksum=39c21495d02f9b5007b4a2d6915496e1 (do not edit this line) */
