/*
 * Copyright (c) 2018 - Arcade Analytics LTD (https://arcadeanalytics.com)
 */

package com.arcadedb.database;

/**
 * Thread local to store transaction data.
 */
public class DatabaseContext extends ThreadLocal<DatabaseContext.PDatabaseContextTL> {
  public void init(final DatabaseInternal database) {
    set(new PDatabaseContextTL());
    get().transaction = new TransactionContext(database);
  }

  public static class PDatabaseContextTL {
    public TransactionContext transaction;
    public Binary             temporaryBuffer1 = new Binary(8196);
    public Binary             temporaryBuffer2 = new Binary(8196);
  }

  public static volatile DatabaseContext INSTANCE = new DatabaseContext();
}
