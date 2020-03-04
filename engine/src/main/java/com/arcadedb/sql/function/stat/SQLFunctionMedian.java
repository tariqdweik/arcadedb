/*
 * Copyright (c) - Arcade Data LTD (https://arcadedata.com)
 */
package com.arcadedb.sql.function.stat;

/**
 * Computes the median for a field. Nulls are ignored in the calculation.
 * 
 * Extends and forces the {@link SQLFunctionPercentile} with the 50th percentile.
 * 
 * @author Fabrizio Fortino
 */
public class SQLFunctionMedian extends SQLFunctionPercentile {

  public static final String NAME = "median";

  public SQLFunctionMedian() {
    super(NAME, 1, 1);
    this.quantiles.add(.5);
  }

  @Override
  public String getSyntax() {
    return NAME + "(<field>)";
  }

}
