/*
 * Copyright (c) - Arcade Data LTD (https://arcadedata.com)
 */

package com.arcadedb.database.async;

import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.exception.ConcurrentModificationException;

public class DatabaseAsyncTransaction extends DatabaseAsyncAbstractTask {
  public final Database.TransactionScope tx;
  public final int                       retries;
  private      OkCallback                onOkCallback;
  private      ErrorCallback             onErrorCallback;

  public DatabaseAsyncTransaction(final Database.TransactionScope tx, final int retries, final OkCallback okCallback, final ErrorCallback errorCallback) {
    this.tx = tx;
    this.retries = retries;
    this.onOkCallback = okCallback;
    this.onErrorCallback = errorCallback;
  }

  @Override
  public boolean requiresActiveTx() {
    return false;
  }

  @Override
  public void execute(final DatabaseAsyncExecutor.AsyncThread async, final DatabaseInternal database) {
    ConcurrentModificationException lastException = null;

    if (database.isTransactionActive())
      database.commit();

    for (int retry = 0; retry < retries + 1; ++retry) {
      try {
        database.begin();
        tx.execute(database);
        database.commit();

        lastException = null;

        if (onOkCallback != null)
          onOkCallback.call();

        // OK
        break;

      } catch (ConcurrentModificationException e) {
        // RETRY
        lastException = e;

        continue;
      } catch (Exception e) {
        if (database.getTransaction().isActive())
          database.rollback();

        async.onError(e);

        if (onErrorCallback != null)
          onErrorCallback.call(e);

        throw e;
      }
    }

    if (lastException != null)
      async.onError(lastException);
  }

  @Override
  public String toString() {
    return "Transaction(" + tx + ")";
  }
}
