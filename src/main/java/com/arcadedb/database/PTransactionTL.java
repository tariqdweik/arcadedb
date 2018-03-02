package com.arcadedb.database;

/**
 * Thread local to store transaction data.
 */
public class PTransactionTL extends ThreadLocal<PTransactionContext> {
  public static volatile PTransactionTL INSTANCE = new PTransactionTL();
}
