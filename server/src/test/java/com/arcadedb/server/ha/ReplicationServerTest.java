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

public class ReplicationServerTest extends BaseGraphServerTest {
  private final int TXS             = 1000;
  private final int VERTICES_PER_TX = 1000;

  public ReplicationServerTest() {
    GlobalConfiguration.HA_REPLICATION_INCOMING_PORTS.setValue("2424-2500");
  }

  protected int getServerCount() {
    return 3;
  }

  @Test
  public void testReplication() {
    for (int s = 0; s < getServerCount(); ++s) {
      Database db = getServer(s).getDatabase(getDatabaseName());
      db.begin();
      try {
        Assertions.assertEquals(1, db.countType(VERTEX1_TYPE_NAME, true), "Check for vertex count for server" + s);
        Assertions.assertEquals(2, db.countType(VERTEX2_TYPE_NAME, true), "Check for vertex count for server" + s);

        Assertions.assertEquals(1, db.countType(EDGE1_TYPE_NAME, true), "Check for edge count for server" + s);
        Assertions.assertEquals(2, db.countType(EDGE2_TYPE_NAME, true), "Check for edge count for server" + s);

      } catch (Exception e) {
        e.printStackTrace();
        Assertions.fail("Error on checking on server" + s);
      } finally {
        db.close();
      }
    }

    Database db = getServer(0).getDatabase(getDatabaseName());
    db.begin();
    try {
      Assertions.assertEquals(1, db.countType(VERTEX1_TYPE_NAME, true), "Check for vertex count for server" + 0);

      LogManager.instance().info(this, "Executing %s transactions with %d vertices each...", TXS, VERTICES_PER_TX);

      long counter = 0;

      for (int tx = 0; tx < TXS; ++tx) {
        for (int i = 0; i < VERTICES_PER_TX; ++i) {
          final ModifiableVertex v1 = db.newVertex(VERTEX1_TYPE_NAME);
          v1.set("id", ++counter);
          v1.set("name", "distributed-test");
          v1.save();
        }

        db.commit();

        if (counter % 100000 == 0)
          LogManager.instance().info(this, "- Progress %d/%d", counter, (TXS * VERTICES_PER_TX));

        db.begin();
      }

      LogManager.instance().info(this, "Done");

      Assertions
          .assertEquals(1 + TXS * VERTICES_PER_TX, db.countType(VERTEX1_TYPE_NAME, true), "Check for vertex count for server" + 0);

    } finally {
      db.close();
    }

    try {
      Thread.sleep(1000);
    } catch (InterruptedException e) {
    }

    for (int s = 0; s < getServerCount(); ++s) {
      db = getServer(s).getDatabase(getDatabaseName());
      db.begin();
      try {
        Assertions.assertEquals(1 + TXS * VERTICES_PER_TX, db.countType(VERTEX1_TYPE_NAME, true),
            "Check for vertex count for server" + s);

        final List<DocumentType.IndexMetadata> indexes = db.getSchema().getType(VERTEX1_TYPE_NAME)
            .getIndexMetadataByProperties("id");
        long total = 0;
        for (int i = 0; i < indexes.size(); ++i) {
          for (IndexCursor it = indexes.get(i).index.iterator(true); it.hasNext(); ) {
            it.next();

            ++total;
          }
        }

        Assertions.assertEquals(1 + TXS * VERTICES_PER_TX, total, "Check for index count for server" + s);

      } catch (Exception e) {
        e.printStackTrace();
        Assertions.fail("Error on checking on server" + s);
      } finally {
        db.close();
      }
    }
  }
}