package com.arcadedb;

import com.arcadedb.database.PDatabase;
import com.arcadedb.database.PDatabaseFactory;
import com.arcadedb.database.PRID;
import com.arcadedb.engine.PFile;
import com.arcadedb.graph.PImmutableEdge3;
import com.arcadedb.graph.PEdge;
import com.arcadedb.graph.PModifiableEdge;
import com.arcadedb.graph.PModifiableVertex;
import com.arcadedb.graph.PVertex;
import com.arcadedb.utility.PFileUtils;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

public class GraphTest {
  private static final int    TOT               = 10000;
  private static final String VERTEX1_TYPE_NAME = "V1";
  private static final String VERTEX2_TYPE_NAME = "V2";
  private static final String EDGE1_TYPE_NAME   = "E1";
  private static final String EDGE2_TYPE_NAME   = "E2";
  private static final String DB_PATH           = "target/database/graph";

  private static PRID root;

  @BeforeAll
  public static void populate() {
    PFileUtils.deleteRecursively(new File(DB_PATH));

    new PDatabaseFactory(DB_PATH, PFile.MODE.READ_WRITE).execute(new PDatabaseFactory.POperation() {
      @Override
      public void execute(PDatabase database) {
        Assertions.assertFalse(database.getSchema().existsType(VERTEX1_TYPE_NAME));
        database.getSchema().createVertexType(VERTEX1_TYPE_NAME, 3);

        Assertions.assertFalse(database.getSchema().existsType(VERTEX2_TYPE_NAME));
        database.getSchema().createVertexType(VERTEX2_TYPE_NAME, 3);

        database.getSchema().createEdgeType(EDGE1_TYPE_NAME);
        database.getSchema().createEdgeType(EDGE2_TYPE_NAME);
      }
    });

    final PDatabase db = new PDatabaseFactory(DB_PATH, PFile.MODE.READ_WRITE).acquire();
    db.begin();
    try {
      final PModifiableVertex v1 = db.newVertex(VERTEX1_TYPE_NAME);
      v1.set("name", "V1");
      v1.save();

      final PModifiableVertex v2 = db.newVertex(VERTEX2_TYPE_NAME);
      v2.set("name", "V2");
      v2.save();

      // CREATION OF EDGE PASSING PARAMS AS VARARGS
      PModifiableEdge e1 = (PModifiableEdge) v1.newEdge(EDGE1_TYPE_NAME, v2, true, "name", "e1");
      Assertions.assertEquals(e1.getOut(), v1);
      Assertions.assertEquals(e1.getIn(), v2);

      final PModifiableVertex v3 = db.newVertex(VERTEX2_TYPE_NAME);
      v3.set("name", "V3");
      v3.save();

      Map<String, Object> params = new HashMap<>();
      params.put("name", "e2");

      // CREATION OF EDGE PASSING PARAMS AS MAP
      PModifiableEdge e2 = (PModifiableEdge) v2.newEdge(EDGE2_TYPE_NAME, v3, true, params);
      Assertions.assertEquals(e2.getOut(), v2);
      Assertions.assertEquals(e2.getIn(), v3);

      PModifiableEdge e3 = (PModifiableEdge) v1.newEdge(EDGE2_TYPE_NAME, v3, true);
      Assertions.assertEquals(e3.getOut(), v1);
      Assertions.assertEquals(e3.getIn(), v3);

      // SETTING EDGE PARAMS AFTER CREATION
      e3.set("name", "e1");
      e3.save();

      db.commit();

      root = v1.getIdentity();

    } finally {
      db.close();
    }
  }

  @AfterAll
  public static void drop() {
    final PDatabase db = new PDatabaseFactory(DB_PATH, PFile.MODE.READ_WRITE).acquire();
    db.drop();
  }

  @Test
  public void checkVertices() throws IOException {
    final PDatabase db2 = new PDatabaseFactory(DB_PATH, PFile.MODE.READ_ONLY).acquire();
    db2.begin();
    try {

      Assertions.assertEquals(1, db2.countType(VERTEX1_TYPE_NAME));
      Assertions.assertEquals(2, db2.countType(VERTEX2_TYPE_NAME));

      final PVertex v1 = (PVertex) db2.lookupByRID(root, false);
      Assertions.assertNotNull(v1);

      // TEST CONNECTED VERTICES
      Assertions.assertEquals(VERTEX1_TYPE_NAME, v1.getType());
      Assertions.assertEquals("V1", v1.get("name"));

      final Iterator<PImmutableEdge3> vertices2level = v1.getConnectedVertices(PVertex.DIRECTION.OUT, EDGE1_TYPE_NAME);
      Assertions.assertNotNull(vertices2level);
      Assertions.assertTrue(vertices2level.hasNext());

      final PImmutableEdge3 entry2 = vertices2level.next();

      Assertions.assertEquals(root.getIdentity(), entry2.getSourceVertex());
      Assertions.assertEquals(EDGE1_TYPE_NAME, entry2.getTypeName());

      final PVertex v2 = (PVertex) entry2.getTargetVertex().getRecord();
      Assertions.assertNotNull(v2);
      Assertions.assertEquals(VERTEX2_TYPE_NAME, v2.getType());

      Assertions.assertEquals("V2", v2.get("name"));

      final Iterator<PImmutableEdge3> vertices2level2 = v1.getConnectedVertices(PVertex.DIRECTION.OUT, EDGE2_TYPE_NAME);
      Assertions.assertTrue(vertices2level2.hasNext());

      final PImmutableEdge3 entry3 = vertices2level2.next();

      final PVertex v3 = (PVertex) entry3.getTargetVertex().getRecord();
      Assertions.assertNotNull(v3);

      Assertions.assertEquals(VERTEX2_TYPE_NAME, v3.getType());
      Assertions.assertEquals("V3", v3.get("name"));

      final Iterator<PImmutableEdge3> vertices3level = v2.getConnectedVertices(PVertex.DIRECTION.OUT, EDGE2_TYPE_NAME);
      Assertions.assertNotNull(vertices3level);
      Assertions.assertTrue(vertices3level.hasNext());

      final PImmutableEdge3 entry32 = vertices3level.next();

      final PVertex v32 = (PVertex) entry32.getTargetVertex().getRecord();
      Assertions.assertNotNull(v32);
      Assertions.assertEquals(VERTEX2_TYPE_NAME, v32.getType());
      Assertions.assertEquals("V3", v32.get("name"));

      // TEST CONNECTED EDGES
      final Iterator<PEdge> edges = v1.getEdges(PVertex.DIRECTION.OUT, EDGE1_TYPE_NAME);
      Assertions.assertNotNull(edges);

      Assertions.assertTrue(v1.isConnectedTo(v2));
      Assertions.assertTrue(v2.isConnectedTo(v1));
      Assertions.assertTrue(v1.isConnectedTo(v3));
      Assertions.assertTrue(v3.isConnectedTo(v1));
      Assertions.assertTrue(v2.isConnectedTo(v3));

      Assertions.assertFalse(v3.isConnectedTo(v1, PVertex.DIRECTION.OUT));
      Assertions.assertFalse(v3.isConnectedTo(v2, PVertex.DIRECTION.OUT));

    } finally {
      db2.close();
    }
  }

  @Test
  public void checkEdges() throws IOException {
    final PDatabase db2 = new PDatabaseFactory(DB_PATH, PFile.MODE.READ_ONLY).acquire();
    db2.begin();
    try {

      Assertions.assertEquals(1, db2.countType(EDGE1_TYPE_NAME));
      Assertions.assertEquals(2, db2.countType(EDGE2_TYPE_NAME));

      final PVertex v1 = (PVertex) db2.lookupByRID(root, false);
      Assertions.assertNotNull(v1);

      // TEST CONNECTED EDGES
      final Iterator<PImmutableEdge3> vertices2level = v1.getConnectedVertices(PVertex.DIRECTION.OUT, EDGE1_TYPE_NAME);
      Assertions.assertNotNull(vertices2level);
      Assertions.assertTrue(vertices2level.hasNext());

      final PImmutableEdge3 entry2 = vertices2level.next();

      Assertions.assertEquals(root.getIdentity(), entry2.getSourceVertex());
      Assertions.assertEquals(EDGE1_TYPE_NAME, entry2.getTypeName());

      final PVertex v2 = (PVertex) entry2.getTargetVertex().getRecord();
      Assertions.assertNotNull(v2);
      Assertions.assertEquals(VERTEX2_TYPE_NAME, v2.getType());

      Assertions.assertEquals("V2", v2.get("name"));

      final Iterator<PImmutableEdge3> vertices2level2 = v1.getConnectedVertices(PVertex.DIRECTION.OUT, EDGE2_TYPE_NAME);
      Assertions.assertTrue(vertices2level2.hasNext());

      final PImmutableEdge3 entry3 = vertices2level2.next();

      final PVertex v3 = (PVertex) entry3.getTargetVertex().getRecord();
      Assertions.assertNotNull(v3);

      Assertions.assertEquals(VERTEX2_TYPE_NAME, v3.getType());
      Assertions.assertEquals("V3", v3.get("name"));

      final Iterator<PImmutableEdge3> vertices3level = v2.getConnectedVertices(PVertex.DIRECTION.OUT, EDGE2_TYPE_NAME);
      Assertions.assertNotNull(vertices3level);
      Assertions.assertTrue(vertices3level.hasNext());

      final PImmutableEdge3 entry32 = vertices3level.next();

      final PVertex v32 = (PVertex) entry32.getTargetVertex().getRecord();
      Assertions.assertNotNull(v32);
      Assertions.assertEquals(VERTEX2_TYPE_NAME, v32.getType());
      Assertions.assertEquals("V3", v32.get("name"));

      // TEST CONNECTED EDGES
      final Iterator<PEdge> edges = v1.getEdges(PVertex.DIRECTION.OUT, EDGE1_TYPE_NAME);
      Assertions.assertNotNull(edges);

      Assertions.assertTrue(v1.isConnectedTo(v2));
      Assertions.assertTrue(v2.isConnectedTo(v1));
      Assertions.assertTrue(v1.isConnectedTo(v3));
      Assertions.assertTrue(v3.isConnectedTo(v1));
      Assertions.assertTrue(v2.isConnectedTo(v3));

      Assertions.assertFalse(v3.isConnectedTo(v1, PVertex.DIRECTION.OUT));
      Assertions.assertFalse(v3.isConnectedTo(v2, PVertex.DIRECTION.OUT));

    } finally {
      db2.close();
    }
  }
}