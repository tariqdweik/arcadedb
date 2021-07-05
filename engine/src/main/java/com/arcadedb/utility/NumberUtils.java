/*
 * Copyright (c) - Arcade Data LTD (https://arcadedata.com)
 */

package com.arcadedb.utility;

public class NumberUtils {

  public static Integer parseInteger(final String s) {
    for (int i = 0; i < s.length(); i++) {
      if (!Character.isDigit(s.charAt(i)))
        return null;
    }
    return Integer.parseInt(s);
  }

}
