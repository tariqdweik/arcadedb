/*
 * Copyright (c) 2018 - Arcade Analytics LTD (https://arcadeanalytics.com)
 */

package com.arcadedb.server.ha;

import com.arcadedb.database.Database;
import com.arcadedb.graph.ModifiableVertex;
import com.arcadedb.server.BaseGraphServerTest;
import com.arcadedb.utility.LogManager;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class ReplicationServerTest extends BaseGraphServerTest {
  private final int TXS             = 1000;
  private final int VERTICES_PER_TX = 10000;

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

      for (int tx = 0; tx < TXS; ++tx) {
        for (int i = 0; i < VERTICES_PER_TX; ++i) {
          final ModifiableVertex v1 = db.newVertex(VERTEX1_TYPE_NAME);
          v1.set("counter", tx * i);
          v1.set("name", "distributed-test");
          v1.save();
        }

        db.commit();
        db.begin();
      }

      LogManager.instance().info(this, "Done");

      Assertions
          .assertEquals(1 + TXS * VERTICES_PER_TX, db.countType(VERTEX1_TYPE_NAME, true), "Check for vertex count for server" + 0);

    } finally {
      db.close();
    }

    try {
      Thread.sleep(1);
    } catch (InterruptedException e) {
    }

    for (int s = 0; s < getServerCount(); ++s) {
      db = getServer(s).getDatabase(getDatabaseName());
      db.begin();
      try {
        Assertions.assertEquals(1 + TXS * VERTICES_PER_TX, db.countType(VERTEX1_TYPE_NAME, true),
            "Check for vertex count for server" + s);

      } catch (Exception e) {
        e.printStackTrace();
        Assertions.fail("Error on checking on server" + s);
      } finally {
        db.close();
      }
    }
  }
}