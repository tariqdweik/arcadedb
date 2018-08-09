/*
 * Copyright (c) 2018 - Arcade Analytics LTD (https://arcadeanalytics.com)
 */

package com.arcadedb.server.ha;

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.database.Database;
import com.arcadedb.graph.ModifiableVertex;
import com.arcadedb.index.IndexCursor;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.server.BaseGraphServerTest;
import com.arcadedb.utility.LogManager;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;

public abstract class ReplicationServerTest extends BaseGraphServerTest {
  public ReplicationServerTest() {
    GlobalConfiguration.HA_REPLICATION_INCOMING_PORTS.setValue("2424-2500");
  }

  protected int getServerCount() {
    return 3;
  }

  protected int getTxs() {
    return 500;
  }

  protected int getVerticesPerTx() {
    return 500;
  }

  @Test
  public void testReplication() {
    checkDatabases();

    Database db = getServerDatabase(0, getDatabaseName());
    db.begin();

    Assertions.assertEquals(1, db.countType(VERTEX1_TYPE_NAME, true), "TEST: Check for vertex count for server" + 0);

    LogManager.instance().info(this, "TEST: Executing %s transactions with %d vertices each...", getTxs(), getVerticesPerTx());

    final long total = getTxs() * getVerticesPerTx();
    long counter = 0;

    for (int tx = 0; tx < getTxs(); ++tx) {
      for (int i = 0; i < getVerticesPerTx(); ++i) {
        final ModifiableVertex v1 = db.newVertex(VERTEX1_TYPE_NAME);
        v1.set("id", ++counter);
        v1.set("name", "distributed-test");
        v1.save();
      }

      db.commit();

      if (counter % (total / 10) == 0) {
        LogManager.instance().info(this, "TEST: - Progress %d/%d", counter, (getTxs() * getVerticesPerTx()));
        if (isPrintingConfigurationAtEveryStep())
          getLeaderServer().getHA().printClusterConfiguration();
      }

      db.begin();
    }

    LogManager.instance().info(this, "Done");

    Assertions.assertEquals(1 + getTxs() * getVerticesPerTx(), db.countType(VERTEX1_TYPE_NAME, true), "Check for vertex count for server" + 0);

    try {
      Thread.sleep(1000);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }

    // CHECK INDEXES ARE REPLICATED CORRECTLY
    for (int s : getServerToCheck()) {
      checkEntriesOnServer(s);
    }

    onAfterTest();
  }

  protected void checkDatabases() {
    for (int s = 0; s < getServerCount(); ++s) {
      Database db = getServerDatabase(s, getDatabaseName());
      db.begin();
      try {
        Assertions.assertEquals(1, db.countType(VERTEX1_TYPE_NAME, true), "Check for vertex count for server" + s);
        Assertions.assertEquals(2, db.countType(VERTEX2_TYPE_NAME, true), "Check for vertex count for server" + s);

        Assertions.assertEquals(1, db.countType(EDGE1_TYPE_NAME, true), "Check for edge count for server" + s);
        Assertions.assertEquals(2, db.countType(EDGE2_TYPE_NAME, true), "Check for edge count for server" + s);

      } catch (Exception e) {
        e.printStackTrace();
        Assertions.fail("Error on checking on server" + s);
      }
    }
  }

  protected void onAfterTest() {
  }

  protected boolean isPrintingConfigurationAtEveryStep() {
    return false;
  }

  protected void checkEntriesOnServer(final int s) {
    final Database db = getServerDatabase(s, getDatabaseName());
    db.begin();
    try {
      Assertions.assertEquals(1 + getTxs() * getVerticesPerTx(), db.countType(VERTEX1_TYPE_NAME, true), "TEST: Check for vertex count for server" + s);

      final List<DocumentType.IndexMetadata> indexes = db.getSchema().getType(VERTEX1_TYPE_NAME).getIndexMetadataByProperties("id");
      long total = 0;
      for (int i = 0; i < indexes.size(); ++i) {
        for (IndexCursor it = indexes.get(i).index.iterator(true); it.hasNext(); ) {
          it.next();
          ++total;
        }
      }

      Assertions.assertEquals(1 + getTxs() * getVerticesPerTx(), total, "TEST: Check for index count for server" + s);

    } catch (Exception e) {
      e.printStackTrace();
      Assertions.fail("TEST: Error on checking on server" + s);
    }
  }
}