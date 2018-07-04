/*
 * Copyright (c) 2018 - Arcade Analytics LTD (https://arcadeanalytics.com)
 */

package com.arcadedb.database.async;

import java.util.Map;

public class DatabaseAsyncSQL implements DatabaseAsyncCommand {
  public final String              command;
  public final Map<String, Object> args;
  public final SQLCallback         userCallback;

  public DatabaseAsyncSQL(final String command, final Map<String, Object> args, final SQLCallback userCallback) {
    this.command = command;
    this.args = args;
    this.userCallback = userCallback;
  }

  @Override
  public String toString() {
    return "SQL(" + command + ")";
  }
}
