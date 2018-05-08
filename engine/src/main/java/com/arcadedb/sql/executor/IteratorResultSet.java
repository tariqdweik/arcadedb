/*
 * Copyright (c) 2018 - Arcade Analytics LTD (https://arcadeanalytics.com)
 */

package com.arcadedb.sql.executor;

import com.arcadedb.database.Document;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Optional;

/**
 * Created by luigidellaquila on 07/07/16.
 */
public class IteratorResultSet implements ResultSet {
  protected final Iterator iterator;

  public IteratorResultSet(Iterator iter) {
    this.iterator = iter;
  }

  @Override public boolean hasNext() {
    return iterator.hasNext();
  }

  @Override public Result next() {
    Object val = iterator.next();
    if (val instanceof Result) {
      return (Result) val;
    }

    ResultInternal result = new ResultInternal();
    if (val instanceof Document) {
      result.setElement((Document) val);
    } else {
      result.setProperty("value", val);
    }
    return result;
  }

  @Override public void close() {

  }

  @Override public Optional<ExecutionPlan> getExecutionPlan() {
    return Optional.empty();
  }

  @Override public Map<String, Long> getQueryStats() {
    return new HashMap<>();
  }

}
