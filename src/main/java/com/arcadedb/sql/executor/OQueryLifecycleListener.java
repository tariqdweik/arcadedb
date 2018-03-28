package com.arcadedb.sql.executor;

public interface OQueryLifecycleListener {
  void queryClosed(String queryId);
}
