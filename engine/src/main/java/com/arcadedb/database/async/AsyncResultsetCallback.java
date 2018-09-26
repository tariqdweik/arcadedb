/*
 * Copyright (c) 2018 - Arcade Analytics LTD (https://arcadeanalytics.com)
 */

package com.arcadedb.database.async;

import com.arcadedb.sql.executor.ResultSet;

public interface AsyncResultsetCallback {
  void onOk(ResultSet resultset);

  void onError(Exception exception);
}
