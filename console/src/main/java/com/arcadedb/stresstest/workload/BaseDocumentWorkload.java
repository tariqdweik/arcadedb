/*
 * Copyright (c) - Arcade Analytics LTD (https://arcadeanalytics.com)
 */
package com.arcadedb.stresstest.workload;

import com.arcadedb.database.Database;
import com.arcadedb.stresstest.DatabaseIdentifier;

/**
 * CRUD implementation of the workload.
 *
 * @author Luca Garulli (l.garulli--(at)--orientdb.com)
 */
public abstract class BaseDocumentWorkload extends OBaseWorkload {
  public class OWorkLoadContext extends OBaseWorkLoadContext {
    private Database db;

    @Override
    public void init(final DatabaseIdentifier dbIdentifier, int operationsPerTransaction) {
      db = dbIdentifier.getEmbeddedDatabase();
    }

    @Override
    public void close() {
      if (getDb() != null)
        getDb().close();
    }

    public Database getDb() {
      return db;
    }
  }

  @Override
  protected OBaseWorkLoadContext getContext() {
    return new OWorkLoadContext();
  }

  @Override
  protected void beginTransaction(final OBaseWorkLoadContext context) {
    ((OWorkLoadContext) context).db.begin();
  }

  @Override
  protected void commitTransaction(final OBaseWorkLoadContext context) {
    ((OWorkLoadContext) context).db.commit();
  }
}
