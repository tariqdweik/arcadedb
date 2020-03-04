/*
 * Copyright (c) - Arcade Data LTD (https://arcadedata.com)
 */
package com.arcadedb.sql.function.stat;

/**
 * Compute the standard deviation for a given field.
 *
 * @author Fabrizio Fortino
 */
public class SQLFunctionStandardDeviation extends SQLFunctionVariance {

  public static final String NAME = "stddev";

  public SQLFunctionStandardDeviation() {
    super(NAME, 1, 1);
  }

  @Override
  public Object getResult() {
    return this.evaluate(super.getResult());
  }

  @Override
  public String getSyntax() {
    return NAME + "(<field>)";
  }

  private Double evaluate(Object variance) {
    Double result = null;
    if (variance != null) {
      result = Math.sqrt((Double) variance);
    }

    return result;
  }
}
