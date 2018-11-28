/*
 * Copyright (c) 2018 - Arcade Analytics LTD (https://arcadeanalytics.com)
 */

package com.arcadedb.importer;

import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.database.DatabaseInternal;

public class CSVImporterMain {
  public static void main(String args[]) {
    final DatabaseInternal database = (DatabaseInternal) new DatabaseFactory("/personal/Development/arcadedb/databases/imported").open();

    database.begin();
    database.getGraphEngine().createIncomingConnectionsInBatch(database, "User", "Follows");
    database.commit();
  }
}
