/*
 * Copyright (c) - Arcade Analytics LTD (https://arcadeanalytics.com)
 */

package com.arcadedb.network.binary;

import com.arcadedb.exception.NeedRetryException;

public class QuorumNotReachedException extends NeedRetryException {
  public QuorumNotReachedException(final String s) {
    super(s);
  }

  public QuorumNotReachedException(String s, Throwable e) {
    super(s, e);
  }
}
