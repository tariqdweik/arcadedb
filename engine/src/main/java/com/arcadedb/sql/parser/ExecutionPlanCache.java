/*
 * Copyright (c) - Arcade Data LTD (https://arcadedata.com)
 */

package com.arcadedb.sql.parser;

import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.sql.executor.CommandContext;
import com.arcadedb.sql.executor.ExecutionPlan;
import com.arcadedb.sql.executor.InternalExecutionPlan;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * This class is an LRU cache for already prepared SQL execution plans. It stores itself in the storage as a resource. It also acts
 * an an entry point for the SQL executor.
 *
 * @author Luigi Dell'Aquila (l.dellaquila-(at)-orientdb.com)
 */
public class ExecutionPlanCache {

  private final DatabaseInternal                   db;
  private final Map<String, InternalExecutionPlan> map;
  private final int                                mapSize;

  protected long lastInvalidation = -1;

  /**
   * @param size the size of the cache
   */
  public ExecutionPlanCache(final DatabaseInternal db, final int size) {
    this.db = db;
    this.mapSize = size;
    this.map = new LinkedHashMap<String, InternalExecutionPlan>(size) {
      protected boolean removeEldestEntry(final Map.Entry<String, InternalExecutionPlan> eldest) {
        return super.size() > mapSize;
      }
    };
  }

  public long getLastInvalidation() {
    final ExecutionPlanCache resource = db.getExecutionPlanCache();
    synchronized (resource) {
      return resource.lastInvalidation;
    }
  }

  /**
   * @param statement an SQL statement
   *
   * @return true if the corresponding executor is present in the cache
   */
  public boolean contains(final String statement) {
    synchronized (map) {
      return map.containsKey(statement);
    }
  }

  /**
   * returns an already prepared SQL execution plan, taking it from the cache if it exists or creating a new one if it doesn't
   *
   * @param statement the SQL statement
   * @param ctx
   *
   * @return a statement executor from the cache
   */
  public ExecutionPlan get(final String statement, final CommandContext ctx) {
    InternalExecutionPlan result;
    synchronized (map) {
      //LRU
      result = map.remove(statement);
      if (result != null) {
        map.put(statement, result);
        result = result.copy(ctx);
      }
    }

    return result;
  }

  public void put(final String statement, final ExecutionPlan plan) {
    synchronized (map) {
      InternalExecutionPlan internal = (InternalExecutionPlan) plan;
      internal = internal.copy(null);
      map.put(statement, internal);
    }
  }

  public void invalidate() {
    synchronized (this) {
      synchronized (map) {
        map.clear();
      }
      lastInvalidation = System.currentTimeMillis();
    }
  }
}
