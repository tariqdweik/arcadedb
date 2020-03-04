/*
 * Copyright (c) - Arcade Data LTD (https://arcadedata.com)
 */
package com.arcadedb.database.async;

public interface ErrorCallback {
  void call(Exception exception);
}
