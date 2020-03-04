/*
 * Copyright (c) - Arcade Data LTD (https://arcadedata.com)
 */

package com.arcadedb.database.async;

import com.arcadedb.sql.executor.ResultSet;

public interface AsyncResultsetCallback {
  void onOk(ResultSet resultset);

  void onError(Exception exception);
}
