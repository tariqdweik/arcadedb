/*
 * Copyright (c) 2018 - Arcade Analytics LTD (https://arcadeanalytics.com)
 */

package com.arcadedb.server.ha;

import com.arcadedb.database.Database;
import com.arcadedb.graph.ModifiableVertex;
import com.arcadedb.server.BaseGraphServerTest;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class ReplicationServerTest extends BaseGraphServerTest {
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

      final ModifiableVertex v1 = db.newVertex(VERTEX1_TYPE_NAME);
      v1.set("name", "distributed-test");
      v1.save();

      db.commit();

      Assertions.assertEquals(2, db.countType(VERTEX1_TYPE_NAME, true), "Check for vertex count for server" + 0);

    } finally {
      db.close();
    }

    for (int s = 0; s < getServerCount(); ++s) {
      db = getServer(s).getDatabase(getDatabaseName());
      db.begin();
      try {
//        Assertions.assertEquals(2, db.countType(VERTEX1_TYPE_NAME, true), "Check for vertex count for server" + s);

      } catch (Exception e) {
        e.printStackTrace();
        Assertions.fail("Error on checking on server" + s);
      } finally {
        db.close();
      }
    }
  }
}