package com.arcadedb.sql.parser;

import com.arcadedb.database.PIdentifiable;
import com.arcadedb.sql.executor.OCommandContext;

import java.util.Collections;
import java.util.Map;

/**
 * @author Luigi Dell'Aquila (l.dellaquila-(at)-orientdb.com)
 */
public class MatchPathItemFirst extends MatchPathItem {
  protected FunctionCall function;

  protected MethodCall methodWrapper;

  public MatchPathItemFirst(int id) {
    super(id);
  }

  public MatchPathItemFirst(OrientSql p, int id) {
    super(p, id);
  }

  public boolean isBidirectional() {
    return false;
  }

  public void toString(Map<Object, Object> params, StringBuilder builder) {

    function.toString(params, builder);
    if (filter != null) {
      filter.toString(params, builder);
    }
  }

  protected Iterable<PIdentifiable> traversePatternEdge(MatchStatement.MatchContext matchContext, PIdentifiable startingPoint,
      OCommandContext iCommandContext) {
    Object qR = this.function.execute(startingPoint, iCommandContext);
    return (qR instanceof Iterable) ? (Iterable) qR : Collections.singleton((PIdentifiable) qR);
  }

  @Override
  public MatchPathItem copy() {
    MatchPathItemFirst result = (MatchPathItemFirst) super.copy();
    result.function = function == null ? null : function.copy();
    return result;
  }

  @Override
  public boolean equals(Object o) {
    if (!super.equals(o)) {
      return false;
    }
    MatchPathItemFirst that = (MatchPathItemFirst) o;

    if (function != null ? !function.equals(that.function) : that.function != null)
      return false;

    return true;
  }

  @Override
  public int hashCode() {
    int result = super.hashCode();
    result = 31 * result + (function != null ? function.hashCode() : 0);
    return result;
  }

  public FunctionCall getFunction() {
    return function;
  }

  public void setFunction(FunctionCall function) {
    this.function = function;
  }

  @Override
  public MethodCall getMethod() {
    if (methodWrapper == null) {
      synchronized (this) {
        if (methodWrapper == null) {
          methodWrapper = new MethodCall(-1);
          methodWrapper.params = function.params;
          methodWrapper.methodName = function.name;
        }
      }
    }
    return methodWrapper;
  }
}
