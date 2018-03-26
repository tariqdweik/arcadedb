package com.arcadedb.sql.parser;

import com.arcadedb.database.PDatabase;
import com.arcadedb.sql.executor.OCommandContext;
import com.arcadedb.sql.executor.OExecutionPlan;
import com.arcadedb.sql.executor.OInternalExecutionPlan;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * This class is an LRU cache for already prepared SQL execution plans. It stores itself in the storage as a resource. It also acts
 * an an entry point for the SQL executor.
 *
 * @author Luigi Dell'Aquila (l.dellaquila-(at)-orientdb.com)
 */
//TODO BIND THE CACHE TO THE DB AND UNCOMMENT STUFF
public class OExecutionPlanCache /*implements OMetadataUpdateListener*/ {

  Map<String, OInternalExecutionPlan> map;
  int                                 mapSize;

  protected long lastInvalidation = -1;

  /**
   * @param size the size of the cache
   */
  public OExecutionPlanCache(int size) {
    this.mapSize = size;
    map = new LinkedHashMap<String, OInternalExecutionPlan>(size) {
      protected boolean removeEldestEntry(final Map.Entry<String, OInternalExecutionPlan> eldest) {
        return super.size() > mapSize;
      }
    };
  }

  public static long getLastInvalidation(PDatabase db) {
    if (db == null) {
      throw new IllegalArgumentException("DB cannot be null");
    }

//    OExecutionPlanCache resource = db.getSharedContext().getExecutionPlanCache();
//    synchronized (resource) {
//      return resource.lastInvalidation;
//    }
    return -1;
  }

  /**
   * @param statement an SQL statement
   *
   * @return true if the corresponding executor is present in the cache
   */
  public boolean contains(String statement) {
    synchronized (map) {
      return map.containsKey(statement);
    }
  }

  /**
   * returns an already prepared SQL execution plan, taking it from the cache if it exists or creating a new one if it doesn't
   *
   * @param statement the SQL statement
   * @param ctx
   * @param db        the current DB instance
   *
   * @return a statement executor from the cache
   */
  public static OExecutionPlan get(String statement, OCommandContext ctx, PDatabase db) {
    if (db == null) {
      throw new IllegalArgumentException("DB cannot be null");
    }

//    OExecutionPlanCache resource = db.getSharedContext().getExecutionPlanCache();
    OExecutionPlanCache resource = instance(db);
    OExecutionPlan result = resource.getInternal(statement, ctx, db);
    return result;
  }

  public static void put(String statement, OExecutionPlan plan, PDatabase db) {
//    if (db == null) {
//      throw new IllegalArgumentException("DB cannot be null");
//    }
//
//    OExecutionPlanCache resource = db.getSharedContext().getExecutionPlanCache();
//    resource.putInternal(statement, plan);
    //TODO
  }

  public void putInternal(String statement, OExecutionPlan plan) {
    synchronized (map) {
      OInternalExecutionPlan internal = (OInternalExecutionPlan) plan;
      internal = internal.copy(null);
      map.put(statement, internal);
    }
  }

  /**
   * @param statement an SQL statement
   * @param ctx
   *
   * @return the corresponding executor, taking it from the internal cache, if it exists
   */
  public OExecutionPlan getInternal(String statement, OCommandContext ctx, PDatabase db) {
    OInternalExecutionPlan result;
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

  public void invalidate() {
    synchronized (this) {
      synchronized (map) {
        map.clear();
      }
      lastInvalidation = System.currentTimeMillis();
    }
  }

//  @Override
//  public void onSchemaUpdate(String database, PSchema schema) {
//    invalidate();
//  }
//
//  @Override
//  public void onIndexManagerUpdate(String database, OIndexManager indexManager) {
//    invalidate();
//  }
//
//  @Override
//  public void onFunctionLibraryUpdate(String database) {
//    invalidate();
//  }
//
//  @Override
//  public void onSequenceLibraryUpdate(String database) {
//    invalidate();
//  }
//
//  @Override
//  public void onStorageConfigurationUpdate(String database, OStorageConfiguration update) {
//    invalidate();
//  }

  public static OExecutionPlanCache instance(PDatabase db) {
    if (db == null) {
      throw new IllegalArgumentException("DB cannot be null");
    }

    return new OExecutionPlanCache(0);
//    OExecutionPlanCache resource = db.getSharedContext().getExecutionPlanCache();
//    return resource;
  }
}
