/*
 * Copyright (c) - Arcade Data LTD (https://arcadedata.com)
 */

package com.arcadedb.sql.executor;

import java.util.Locale;

public class QueryHelper {
  public static boolean like(String s, String s1) {
    if (s == null || s1 == null) {
      return false;
    }
    s1 = s1.toLowerCase(Locale.ENGLISH);
    s1 = s1.replace(".", "\\.");
    s1 = s1.replace("?", ".");
    s1 = s1.replace("%", ".*");
    s = s.toLowerCase(Locale.ENGLISH);
    return s.matches(s1);
  }
}
