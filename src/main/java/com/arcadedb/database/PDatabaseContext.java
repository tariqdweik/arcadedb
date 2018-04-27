package com.arcadedb.database;

/**
 * Thread local to store transaction data.
 */
public class PDatabaseContext extends ThreadLocal<PDatabaseContext.PDatabaseContextTL> {
  public void init(final PDatabaseInternal database) {
    set(new PDatabaseContextTL());
    get().transaction = new PTransactionContext(database);
  }

  public static class PDatabaseContextTL {
    public PTransactionContext transaction;
    public PBinary             temporaryBuffer1 = new PBinary(8196);
    public PBinary             temporaryBuffer2 = new PBinary(8196);
  }

  public static volatile PDatabaseContext INSTANCE = new PDatabaseContext();
}
