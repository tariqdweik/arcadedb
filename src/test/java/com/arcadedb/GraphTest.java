package com.arcadedb;

import com.arcadedb.database.PDatabase;
import com.arcadedb.database.PDatabaseFactory;
import com.arcadedb.database.PRID;
import com.arcadedb.engine.PPaginatedFile;
import com.arcadedb.exception.PRecordNotFoundException;
import com.arcadedb.graph.PEdge;
import com.arcadedb.graph.PModifiableEdge;
import com.arcadedb.graph.PModifiableVertex;
import com.arcadedb.graph.PVertex;
import com.arcadedb.utility.PFileUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

public class GraphTest {
  private static final String VERTEX1_TYPE_NAME = "V1";
  private static final String VERTEX2_TYPE_NAME = "V2";
  private static final String EDGE1_TYPE_NAME   = "E1";
  private static final String EDGE2_TYPE_NAME   = "E2";
  private static final String DB_PATH           = "target/database/graph";

  private static PRID root;

  @BeforeEach
  public void populate() {
    PFileUtils.deleteRecursively(new File(DB_PATH));

    new PDatabaseFactory(DB_PATH, PPaginatedFile.MODE.READ_WRITE).execute(new PDatabaseFactory.POperation() {
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

    final PDatabase db = new PDatabaseFactory(DB_PATH, PPaginatedFile.MODE.READ_WRITE).acquire();
    db.begin();
    try {
      final PModifiableVertex v1 = db.newVertex(VERTEX1_TYPE_NAME);
      v1.set("name", VERTEX1_TYPE_NAME);
      v1.save();

      final PModifiableVertex v2 = db.newVertex(VERTEX2_TYPE_NAME);
      v2.set("name", VERTEX2_TYPE_NAME);
      v2.save();

      // CREATION OF EDGE PASSING PARAMS AS VARARGS
      PModifiableEdge e1 = (PModifiableEdge) v1.newEdge(EDGE1_TYPE_NAME, v2, true, "name", "E1");
      Assertions.assertEquals(e1.getOut(), v1);
      Assertions.assertEquals(e1.getIn(), v2);

      final PModifiableVertex v3 = db.newVertex(VERTEX2_TYPE_NAME);
      v3.set("name", "V3");
      v3.save();

      Map<String, Object> params = new HashMap<>();
      params.put("name", "E2");

      // CREATION OF EDGE PASSING PARAMS AS MAP
      PModifiableEdge e2 = (PModifiableEdge) v2.newEdge(EDGE2_TYPE_NAME, v3, true, params);
      Assertions.assertEquals(e2.getOut(), v2);
      Assertions.assertEquals(e2.getIn(), v3);

      PModifiableEdge e3 = (PModifiableEdge) v1.newEdge(EDGE2_TYPE_NAME, v3, true);
      Assertions.assertEquals(e3.getOut(), v1);
      Assertions.assertEquals(e3.getIn(), v3);

      db.commit();

      root = v1.getIdentity();

    } finally {
      db.close();
    }
  }

  @AfterEach
  public void drop() {
    final PDatabase db = new PDatabaseFactory(DB_PATH, PPaginatedFile.MODE.READ_WRITE).acquire();
    db.drop();
  }

  @Test
  public void checkVertices() {
    final PDatabase db2 = new PDatabaseFactory(DB_PATH, PPaginatedFile.MODE.READ_ONLY).acquire();
    db2.begin();
    try {

      Assertions.assertEquals(1, db2.countType(VERTEX1_TYPE_NAME));
      Assertions.assertEquals(2, db2.countType(VERTEX2_TYPE_NAME));

      final PVertex v1 = (PVertex) db2.lookupByRID(root, false);
      Assertions.assertNotNull(v1);

      // TEST CONNECTED VERTICES
      Assertions.assertEquals(VERTEX1_TYPE_NAME, v1.getType());
      Assertions.assertEquals(VERTEX1_TYPE_NAME, v1.get("name"));

      final Iterator<PVertex> vertices2level = v1.getVertices(PVertex.DIRECTION.OUT, EDGE1_TYPE_NAME);
      Assertions.assertNotNull(vertices2level);
      Assertions.assertTrue(vertices2level.hasNext());

      final PVertex v2 = vertices2level.next();

      Assertions.assertNotNull(v2);
      Assertions.assertEquals(VERTEX2_TYPE_NAME, v2.getType());
      Assertions.assertEquals(VERTEX2_TYPE_NAME, v2.get("name"));

      final Iterator<PVertex> vertices2level2 = v1.getVertices(PVertex.DIRECTION.OUT, EDGE2_TYPE_NAME);
      Assertions.assertTrue(vertices2level2.hasNext());

      final PVertex v3 = vertices2level2.next();
      Assertions.assertNotNull(v3);

      Assertions.assertEquals(VERTEX2_TYPE_NAME, v3.getType());
      Assertions.assertEquals("V3", v3.get("name"));

      final Iterator<PVertex> vertices3level = v2.getVertices(PVertex.DIRECTION.OUT, EDGE2_TYPE_NAME);
      Assertions.assertNotNull(vertices3level);
      Assertions.assertTrue(vertices3level.hasNext());

      final PVertex v32 = vertices3level.next();

      Assertions.assertNotNull(v32);
      Assertions.assertEquals(VERTEX2_TYPE_NAME, v32.getType());
      Assertions.assertEquals("V3", v32.get("name"));

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
  public void checkEdges() {
    final PDatabase db2 = new PDatabaseFactory(DB_PATH, PPaginatedFile.MODE.READ_ONLY).acquire();
    db2.begin();
    try {

      Assertions.assertEquals(1, db2.countType(EDGE1_TYPE_NAME));
      Assertions.assertEquals(2, db2.countType(EDGE2_TYPE_NAME));

      final PVertex v1 = (PVertex) db2.lookupByRID(root, false);
      Assertions.assertNotNull(v1);

      // TEST CONNECTED EDGES
      final Iterator<PEdge> edges1 = v1.getEdges(PVertex.DIRECTION.OUT, EDGE1_TYPE_NAME);
      Assertions.assertNotNull(edges1);
      Assertions.assertTrue(edges1.hasNext());

      final PEdge e1 = edges1.next();

      Assertions.assertNotNull(e1);
      Assertions.assertEquals(EDGE1_TYPE_NAME, e1.getType());
      Assertions.assertEquals(v1, e1.getOut());
      Assertions.assertEquals("E1", e1.get("name"));

      PVertex v2 = (PVertex) e1.getIn().getRecord();
      Assertions.assertEquals(VERTEX2_TYPE_NAME, v2.get("name"));

      final Iterator<PEdge> edges2 = v2.getEdges(PVertex.DIRECTION.OUT, EDGE2_TYPE_NAME);
      Assertions.assertTrue(edges2.hasNext());

      final PEdge e2 = edges2.next();
      Assertions.assertNotNull(e2);

      Assertions.assertEquals(EDGE2_TYPE_NAME, e2.getType());
      Assertions.assertEquals(v2, e2.getOut());
      Assertions.assertEquals("E2", e2.get("name"));

      PVertex v3 = (PVertex) e2.getIn().getRecord();
      Assertions.assertEquals("V3", v3.get("name"));

      final Iterator<PEdge> edges3 = v1.getEdges(PVertex.DIRECTION.OUT, EDGE2_TYPE_NAME);
      Assertions.assertNotNull(edges3);
      Assertions.assertTrue(edges3.hasNext());

      final PEdge e3 = edges3.next();

      Assertions.assertNotNull(e3);
      Assertions.assertEquals(EDGE2_TYPE_NAME, e3.getType());
      Assertions.assertEquals(v1, e3.getOut());
      Assertions.assertEquals(v3, e3.getIn());

      v2.getEdges();

    } finally {
      db2.close();
    }
  }

  @Test
  public void deleteVertices() {
    final PDatabase db = new PDatabaseFactory(DB_PATH, PPaginatedFile.MODE.READ_WRITE).acquire();
    db.begin();
    try {

      PVertex v1 = (PVertex) db.lookupByRID(root, false);
      Assertions.assertNotNull(v1);

      Iterator<PVertex> outV = v1.getVertices(PVertex.DIRECTION.OUT);
      Assertions.assertTrue(outV.hasNext());
      PVertex v2 = outV.next();
      Assertions.assertNotNull(v2);

      Assertions.assertTrue(outV.hasNext());
      PVertex v3 = outV.next();
      Assertions.assertNotNull(v3);

      db.deleteRecord(v1);

      // -----------------------
      v2 = (PVertex) db.lookupByRID(v2.getIdentity(), true);

      outV = v2.getVertices(PVertex.DIRECTION.IN);
      Assertions.assertFalse(outV.hasNext());

      outV = v2.getVertices(PVertex.DIRECTION.OUT);
      Assertions.assertTrue(outV.hasNext());

      v3 = (PVertex) db.lookupByRID(v3.getIdentity(), true);

      // Expecting 1 edge only: V2 is still connected to V3
      outV = v3.getVertices(PVertex.DIRECTION.IN);
      Assertions.assertTrue(outV.hasNext());
      outV.next();
      Assertions.assertFalse(outV.hasNext());

      try {
        db.lookupByRID(root, true);
        Assertions.fail("Expected deleted record");
      } catch (PRecordNotFoundException e) {
      }

    } finally {
      db.close();
    }
  }
}