/*
 * Copyright (c) 2018 - Arcade Analytics LTD (https://arcadeanalytics.com)
 */

package com.arcadedb.graph;

import java.io.IOException;

public class GraphAlgorithmException extends RuntimeException {
  public GraphAlgorithmException(final String s) {
    super(s);
  }

  public GraphAlgorithmException(String s, IOException e) {
    super(s, e);
  }
}
