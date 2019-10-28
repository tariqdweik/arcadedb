/*
 * Copyright (c) - Arcade Analytics LTD (https://arcadeanalytics.com)
 */
package com.arcadedb.database.async;

public interface ErrorCallback {
  void call(Exception exception);
}
