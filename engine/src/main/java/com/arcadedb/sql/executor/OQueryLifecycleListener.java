/*
 * Copyright (c) 2018 - Arcade Analytics LTD (https://arcadeanalytics.com)
 */

package com.arcadedb.sql.executor;

public interface OQueryLifecycleListener {
  void queryClosed(String queryId);
}
