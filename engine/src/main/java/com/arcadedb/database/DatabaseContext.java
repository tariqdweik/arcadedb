/*
 * Copyright (c) 2018 - Arcade Analytics LTD (https://arcadeanalytics.com)
 */

package com.arcadedb.database;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Thread local to store transaction data.
 */
public class DatabaseContext extends ThreadLocal<Map<String, DatabaseContext.DatabaseContextTL>> {
  public DatabaseContextTL init(final DatabaseInternal database) {
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
        if (!current.transaction.isEmpty()) {
          // ROLLBACK PREVIOUS TXS
          while (!current.transaction.isEmpty()) {
            final Transaction tx = current.transaction.remove(current.transaction.size() - 1);
            tx.rollback();
          }
        }
      }
    }

    if (current.transaction.isEmpty())
      current.transaction.add(new TransactionContext(database.getWrappedDatabaseInstance()));

    return current;
  }

  public DatabaseContextTL getContext(final String name) {
    final Map<String, DatabaseContextTL> map = get();
    return map != null ? map.get(name) : null;
  }

  public DatabaseContextTL removeContext(final String name) {
    final Map<String, DatabaseContextTL> map = get();
    if (map != null)
      return map.remove(name);
    return null;
  }

  public static class DatabaseContextTL {
    public final List<TransactionContext> transaction = new ArrayList<>(1);
    public       boolean                  asyncMode   = false;
    private      Binary                   temporaryBuffer1;
    private      Binary                   temporaryBuffer2;

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

    public TransactionContext getLastTransaction() {
      if (transaction.isEmpty())
        return null;
      return transaction.get(transaction.size() - 1);
    }

    public void pushTransaction(final TransactionContext tx) {
      transaction.add(tx);
    }

    public TransactionContext popIfNotLastTransaction() {
      if (transaction.isEmpty())
        return null;

      if (transaction.size() > 1)
        return transaction.remove(transaction.size() - 1);

      return transaction.get(0);
    }
  }

  public static volatile DatabaseContext INSTANCE = new DatabaseContext();
}
