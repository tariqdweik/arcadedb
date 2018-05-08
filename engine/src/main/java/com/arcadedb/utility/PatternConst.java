/*
 * Copyright (c) 2018 - Arcade Analytics LTD (https://arcadeanalytics.com)
 */
package com.arcadedb.utility;

import java.util.regex.Pattern;

public final class PatternConst {

  public static final Pattern PATTERN_COMMA_SEPARATED   = Pattern.compile("\\s*,\\s*");
  public static final Pattern PATTERN_SPACES            = Pattern.compile("\\s+");
  public static final Pattern PATTERN_FETCH_PLAN        = Pattern.compile(".*:-?\\d+");
  public static final Pattern PATTERN_SINGLE_SPACE      = Pattern.compile(" ");
  public static final Pattern PATTERN_NUMBERS           = Pattern.compile("[^\\d]");
  public static final Pattern PATTERN_RID               = Pattern.compile("#(-?[0-9]+):(-?[0-9]+)");
  public static final Pattern PATTERN_DIACRITICAL_MARKS = Pattern.compile("\\p{InCombiningDiacriticalMarks}+");
  public static final Pattern PATTERN_AMP               = Pattern.compile("&");
  public static final Pattern PATTERN_REST_URL          = Pattern.compile("\\{[a-zA-Z0-9%:]*\\}");

  private PatternConst() {
  }
}
