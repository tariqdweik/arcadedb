/*
 * Copyright (c) 2018 - Arcade Analytics LTD (https://arcadeanalytics.com)
 */

package com.arcadedb.database;

import java.util.HashMap;
import java.util.Map;

/**
 * Thread local to store transaction data.
 */
public class DatabaseContext extends ThreadLocal<Map<String, DatabaseContext.DatabaseContextTL>> {
  public void init(final DatabaseInternal database) {
    Map<String, DatabaseContextTL> map = get();

    final String key = database.getDatabasePath();

    DatabaseContextTL current;

    if (map == null) {
      map = new HashMap<>();
      set(map);
      current = new DatabaseContextTL();
      map.put(key, current);
    } else {
      current = map.get(key);
      if (current == null) {
        current = new DatabaseContextTL();
        map.put(key, current);
      } else {
        if (current.transaction != null) {
          // ROLLBACK PREVIOUS TX
          final TransactionContext tx = current.transaction;
          current.transaction = null;
          tx.rollback();
        }
      }
    }

    current.transaction = new TransactionContext(database);
  }

  public DatabaseContextTL getContext(final String name) {
    final Map<String, DatabaseContextTL> map = get();
    return map != null ? map.get(name) : null;
  }

  public static class DatabaseContextTL {
    public  boolean            asyncMode = false;
    public  TransactionContext transaction;
    private Binary             temporaryBuffer1;
    private Binary             temporaryBuffer2;

    public Binary getTemporaryBuffer1() {
      if (temporaryBuffer1 == null)
        temporaryBuffer1 = new Binary(8196);
      temporaryBuffer1.clear();
      return temporaryBuffer1;
    }

    public Binary getTemporaryBuffer2() {
      if (temporaryBuffer2 == null)
        temporaryBuffer2 = new Binary(8196);
      temporaryBuffer2.clear();
      return temporaryBuffer2;
    }
  }

  public static volatile DatabaseContext INSTANCE = new DatabaseContext();
}
