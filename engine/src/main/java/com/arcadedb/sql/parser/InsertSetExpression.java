/*
 * Copyright (c) - Arcade Data LTD (https://arcadedata.com)
 */

package com.arcadedb.sql.parser;

import java.util.Map;

/**
 * Created by luigidellaquila on 19/02/15.
 */
public class InsertSetExpression {

  protected Identifier left;
  protected Expression right;

  public void toString(Map<Object, Object> params, StringBuilder builder) {
    left.toString(params, builder);
    builder.append(" = ");
    right.toString(params, builder);

  }

  public InsertSetExpression copy() {
    InsertSetExpression result = new InsertSetExpression();
    result.left = left == null ? null : left.copy();
    result.right = right == null ? null : right.copy();
    return result;
  }

  public Identifier getLeft() {
    return left;
  }

  public Expression getRight() {
    return right;
  }
}

