package com.arcadedb.sql.parser;

import com.arcadedb.sql.executor.OResult;
import com.arcadedb.sql.executor.OResultInternal;

import java.util.Map;
import java.util.Set;

/**
 * Created by luigidellaquila on 18/02/15.
 */
public class JsonItem {
  protected Identifier leftIdentifier;
  protected String     leftString;
  protected Expression right;

  public void toString(Map<Object, Object> params, StringBuilder builder) {
    if (leftIdentifier != null) {
      builder.append("\"");
      leftIdentifier.toString(params, builder);
      builder.append("\"");
    }
    if (leftString != null) {
      builder.append("\"");
      builder.append(Expression.encode(leftString));
      builder.append("\"");
    }
    builder.append(": ");
    right.toString(params, builder);
  }

  public String getLeftValue() {
    if (leftString != null) {
      return leftString;
    }
    if (leftIdentifier != null) {
      return leftIdentifier.getStringValue();
    }
    return null;
  }

  public boolean needsAliases(Set<String> aliases) {
    if (aliases.contains(leftIdentifier.getStringValue())) {
      return true;
    }
    if (right.needsAliases(aliases)) {
      return true;
    }
    return false;
  }

  public boolean isAggregate() {
    return right.isAggregate();
  }

  public JsonItem splitForAggregation(AggregateProjectionSplit aggregateSplit) {
    if (isAggregate()) {
      JsonItem item = new JsonItem();
      item.leftIdentifier = leftIdentifier;
      item.leftString = leftString;
      item.right = right.splitForAggregation(aggregateSplit);
      return item;
    } else {
      return this;
    }
  }

  public JsonItem copy() {
    JsonItem result = new JsonItem();
    result.leftIdentifier = leftIdentifier == null ? null : leftIdentifier.copy();
    result.leftString = leftString;
    result.right = right.copy();
    return result;
  }

  public void extractSubQueries(SubQueryCollector collector) {
    right.extractSubQueries(collector);
  }

  public boolean refersToParent() {
    return right != null && right.refersToParent();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o)
      return true;
    if (o == null || getClass() != o.getClass())
      return false;

    JsonItem oJsonItem = (JsonItem) o;

    if (leftIdentifier != null ? !leftIdentifier.equals(oJsonItem.leftIdentifier) : oJsonItem.leftIdentifier != null)
      return false;
    if (leftString != null ? !leftString.equals(oJsonItem.leftString) : oJsonItem.leftString != null)
      return false;
    return right != null ? right.equals(oJsonItem.right) : oJsonItem.right == null;
  }

  @Override
  public int hashCode() {
    int result = leftIdentifier != null ? leftIdentifier.hashCode() : 0;
    result = 31 * result + (leftString != null ? leftString.hashCode() : 0);
    result = 31 * result + (right != null ? right.hashCode() : 0);
    return result;
  }

  public OResult serialize() {
    OResultInternal result = new OResultInternal();
    result.setProperty("leftIdentifier", leftIdentifier.serialize());
    result.setProperty("leftString", leftString);
    result.setProperty("right", right.serialize());
    return result;
  }

  public void deserialize(OResult fromResult) {
    if (fromResult.getProperty("leftIdentifier") != null) {
      leftIdentifier = new Identifier(-1);
      leftIdentifier.deserialize(fromResult.getProperty("leftIdentifier"));
    }
    if (fromResult.getProperty("leftString") != null) {
      leftString = fromResult.getProperty("leftString");
    }
    if (fromResult.getProperty("right") != null) {
      right = new Expression(-1);
      right.deserialize(fromResult.getProperty("right"));
    }

  }

}
