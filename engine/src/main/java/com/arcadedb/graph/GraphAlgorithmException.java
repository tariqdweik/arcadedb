/*
 * Copyright (c) - Arcade Data LTD (https://arcadedata.com)
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
