package com.arcadedb.sql.parser;

import com.arcadedb.sql.executor.OExecutionPlan;
import com.arcadedb.sql.executor.OInternalExecutionPlan;
import com.arcadedb.sql.executor.OResult;
import com.arcadedb.sql.executor.OResultSet;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

/**
 * Created by luigidellaquila on 07/07/16.
 */
public class OLocalResultSet implements OResultSet {

  private OResultSet lastFetch = null;
  private final OInternalExecutionPlan executionPlan;
  private boolean finished = false;

  public OLocalResultSet(OInternalExecutionPlan executionPlan) {
    this.executionPlan = executionPlan;
    fetchNext();
  }

  private boolean fetchNext() {
    lastFetch = executionPlan.fetchNext(100000);
    if (!lastFetch.hasNext()) {
      finished = true;
      return false;
    }
    return true;
  }

  @Override public boolean hasNext() {
    if (finished) {
      return false;
    }
    if (lastFetch.hasNext()) {
      return true;
    } else {
      return fetchNext();
    }
  }

  @Override public OResult next() {
    if (finished) {
      throw new IllegalStateException();
    }
    if (!lastFetch.hasNext()) {
      if (!fetchNext()) {
        throw new IllegalStateException();
      }
    }
    return lastFetch.next();
  }

  @Override public void close() {
    executionPlan.close();
  }

  @Override public Optional<OExecutionPlan> getExecutionPlan() {
    return Optional.of(executionPlan);
  }

  @Override public Map<String, Long> getQueryStats() {
    return new HashMap<>();//TODO
  }

}
