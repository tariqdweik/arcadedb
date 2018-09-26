/*
 * Copyright (c) 2018 - Arcade Analytics LTD (https://arcadeanalytics.com)
 */

package com.arcadedb.database.async;

import java.util.Map;

public class DatabaseAsyncCommand extends DatabaseAsyncAbstractTask {
  public final boolean                idempotent;
  public final String                 language;
  public final String                 command;
  public final Object[]               parameters;
  public final Map<String, Object>    parametersMap;
  public final AsyncResultsetCallback userCallback;

  public DatabaseAsyncCommand(final boolean idempotent, final String language, final String command, final Object[] parameters,
      final AsyncResultsetCallback userCallback) {
    this.idempotent = idempotent;
    this.language = language;
    this.command = command;
    this.parameters = parameters;
    this.parametersMap = null;
    this.userCallback = userCallback;
  }

  public DatabaseAsyncCommand(final boolean idempotent, final String language, final String command, final Map<String, Object> parametersMap,
      final AsyncResultsetCallback userCallback) {
    this.idempotent = idempotent;
    this.language = language;
    this.command = command;
    this.parameters = null;
    this.parametersMap = parametersMap;
    this.userCallback = userCallback;
  }

  @Override
  public String toString() {
    return (idempotent ? "Query" : "Command") + "(" + language + "," + command + ")";
  }
}
