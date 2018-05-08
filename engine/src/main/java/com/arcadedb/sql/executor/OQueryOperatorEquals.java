/*
 * Copyright (c) 2018 - Arcade Analytics LTD (https://arcadeanalytics.com)
 */

package com.arcadedb.sql.executor;

public class OQueryOperatorEquals {
  public static boolean equals(Object o, Object right) {
    if (o == null && right == null) {
      return true;
    }
    if (o == null) {
      return false;
    }
    return o.equals(right);
  }
}
