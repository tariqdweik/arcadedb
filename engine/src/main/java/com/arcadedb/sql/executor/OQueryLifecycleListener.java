/*
 * Copyright (c) - Arcade Data LTD (https://arcadedata.com)
 */

package com.arcadedb.sql.executor;

public interface OQueryLifecycleListener {
  void queryClosed(String queryId);
}
