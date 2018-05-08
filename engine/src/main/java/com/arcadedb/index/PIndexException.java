/*
 * Copyright (c) 2018 - Arcade Analytics LTD (https://arcadeanalytics.com)
 */

package com.arcadedb.index;

import java.io.IOException;

public class PIndexException extends RuntimeException {
  public PIndexException(final String s) {
    super(s);
  }

  public PIndexException(String s, IOException e) {
    super(s, e);
  }
}
