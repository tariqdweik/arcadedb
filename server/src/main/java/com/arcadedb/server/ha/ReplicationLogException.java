/*
 * Copyright (c) - Arcade Analytics LTD (https://arcadeanalytics.com)
 */

package com.arcadedb.server.ha;

public class ReplicationLogException extends ReplicationException {
  public ReplicationLogException(final String s) {
    super(s);
  }

  public ReplicationLogException(String s, Throwable e) {
    super(s, e);
  }
}
