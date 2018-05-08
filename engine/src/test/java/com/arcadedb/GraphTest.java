package com.arcadedb;

import com.arcadedb.database.PDatabase;
import com.arcadedb.database.PDatabaseFactory;
import com.arcadedb.engine.PDatabaseChecker;
import com.arcadedb.engine.PPaginatedFile;
import com.arcadedb.exception.PRecordNotFoundException;
import com.arcadedb.graph.PEdge;
import com.arcadedb.graph.PModifiableEdge;
import com.arcadedb.graph.PModifiableVertex;
import com.arcadedb.graph.PVertex;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Iterator;

public class GraphTest extends BaseGraphTest {

  @Test
  public void checkVertices() {
    final PDatabase db2 = new PDatabaseFactory(DB_PATH, PPaginatedFile.MODE.READ_ONLY).acquire();
    db2.begin();
    try {

      Assertions.assertEquals(1, db2.countType(VERTEX1_TYPE_NAME, false));
      Assertions.assertEquals(2, db2.countType(VERTEX2_TYPE_NAME, false));

      final PVertex v1 = (PVertex) db2.lookupByRID(root, false);
      Assertions.assertNotNull(v1);

      // TEST CONNECTED VERTICES
      Assertions.assertEquals(VERTEX1_TYPE_NAME, v1.getType());
      Assertions.assertEquals(VERTEX1_TYPE_NAME, v1.get("name"));

      final Iterator<PVertex> vertices2level = v1.getVertices(PVertex.DIRECTION.OUT, new String[]{EDGE1_TYPE_NAME}).iterator();
      Assertions.assertNotNull(vertices2level);
      Assertions.assertTrue(vertices2level.hasNext());

      final PVertex v2 = vertices2level.next();

      Assertions.assertNotNull(v2);
      Assertions.assertEquals(VERTEX2_TYPE_NAME, v2.getType());
      Assertions.assertEquals(VERTEX2_TYPE_NAME, v2.get("name"));

      final Iterator<PVertex> vertices2level2 = v1.getVertices(PVertex.DIRECTION.OUT, new String[]{EDGE2_TYPE_NAME}).iterator();
      Assertions.assertTrue(vertices2level2.hasNext());

      final PVertex v3 = vertices2level2.next();
      Assertions.assertNotNull(v3);

      Assertions.assertEquals(VERTEX2_TYPE_NAME, v3.getType());
      Assertions.assertEquals("V3", v3.get("name"));

      final Iterator<PVertex> vertices3level = v2.getVertices(PVertex.DIRECTION.OUT, new String[]{EDGE2_TYPE_NAME}).iterator();
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

      Assertions.assertEquals(1, db2.countType(EDGE1_TYPE_NAME, false));
      Assertions.assertEquals(2, db2.countType(EDGE2_TYPE_NAME, false));

      final PVertex v1 = (PVertex) db2.lookupByRID(root, false);
      Assertions.assertNotNull(v1);

      // TEST CONNECTED EDGES
      final Iterator<PEdge> edges1 = v1.getEdges(PVertex.DIRECTION.OUT, new String[]{EDGE1_TYPE_NAME}).iterator();
      Assertions.assertNotNull(edges1);
      Assertions.assertTrue(edges1.hasNext());

      final PEdge e1 = edges1.next();

      Assertions.assertNotNull(e1);
      Assertions.assertEquals(EDGE1_TYPE_NAME, e1.getType());
      Assertions.assertEquals(v1, e1.getOut());
      Assertions.assertEquals("E1", e1.get("name"));

      PVertex v2 = e1.getInVertex();
      Assertions.assertEquals(VERTEX2_TYPE_NAME, v2.get("name"));

      final Iterator<PEdge> edges2 = v2.getEdges(PVertex.DIRECTION.OUT, new String[]{EDGE2_TYPE_NAME}).iterator();
      Assertions.assertTrue(edges2.hasNext());

      final PEdge e2 = edges2.next();
      Assertions.assertNotNull(e2);

      Assertions.assertEquals(EDGE2_TYPE_NAME, e2.getType());
      Assertions.assertEquals(v2, e2.getOut());
      Assertions.assertEquals("E2", e2.get("name"));

      PVertex v3 = e2.getInVertex();
      Assertions.assertEquals("V3", v3.get("name"));

      final Iterator<PEdge> edges3 = v1.getEdges(PVertex.DIRECTION.OUT, new String[]{EDGE2_TYPE_NAME}).iterator();
      Assertions.assertNotNull(edges3);
      Assertions.assertTrue(edges3.hasNext());

      final PEdge e3 = edges3.next();

      Assertions.assertNotNull(e3);
      Assertions.assertEquals(EDGE2_TYPE_NAME, e3.getType());
      Assertions.assertEquals(v1, e3.getOutVertex());
      Assertions.assertEquals(v3, e3.getInVertex());

      v2.getEdges();

    } finally {
      db2.close();
    }
  }

  @Test
  public void updateVerticesAndEdges() {
    final PDatabase db = new PDatabaseFactory(DB_PATH, PPaginatedFile.MODE.READ_WRITE).acquire();
    db.begin();
    try {

      Assertions.assertEquals(1, db.countType(EDGE1_TYPE_NAME, false));
      Assertions.assertEquals(2, db.countType(EDGE2_TYPE_NAME, false));

      final PVertex v1 = (PVertex) db.lookupByRID(root, false);
      Assertions.assertNotNull(v1);

      final PModifiableVertex v1Copy = (PModifiableVertex) v1.modify();
      v1Copy.set("newProperty1", "TestUpdate1");
      v1Copy.save();

      // TEST CONNECTED EDGES
      final Iterator<PEdge> edges1 = v1.getEdges(PVertex.DIRECTION.OUT, new String[]{EDGE1_TYPE_NAME}).iterator();
      Assertions.assertNotNull(edges1);
      Assertions.assertTrue(edges1.hasNext());

      final PEdge e1 = edges1.next();

      Assertions.assertNotNull(e1);

      final PModifiableEdge e1Copy = (PModifiableEdge) e1.modify();
      e1Copy.set("newProperty2", "TestUpdate2");
      e1Copy.save();

      db.commit();

      final PVertex v1CopyReloaded = (PVertex) db.lookupByRID(v1Copy.getIdentity(), true);
      Assertions.assertEquals("TestUpdate1", v1CopyReloaded.get("newProperty1"));
      final PEdge e1CopyReloaded = (PEdge) db.lookupByRID(e1Copy.getIdentity(), true);
      Assertions.assertEquals("TestUpdate2", e1CopyReloaded.get("newProperty2"));

    } finally {
      new PDatabaseChecker().check(db);
      db.close();
    }
  }

  @Test
  public void deleteVertices() {
    final PDatabase db = new PDatabaseFactory(DB_PATH, PPaginatedFile.MODE.READ_WRITE).acquire();
    db.begin();
    try {

      PVertex v1 = (PVertex) db.lookupByRID(root, false);
      Assertions.assertNotNull(v1);

      Iterator<PVertex> vertices = v1.getVertices(PVertex.DIRECTION.OUT).iterator();
      Assertions.assertTrue(vertices.hasNext());
      PVertex v2 = vertices.next();
      Assertions.assertNotNull(v2);

      Assertions.assertTrue(vertices.hasNext());
      PVertex v3 = vertices.next();
      Assertions.assertNotNull(v3);

      final long totalVertices = db.countType(v1.getType(), true);

      // DELETE THE VERTEX
      // -----------------------
      db.deleteRecord(v1);

      Assertions.assertEquals(totalVertices - 1, db.countType(v1.getType(), true));

      vertices = v2.getVertices(PVertex.DIRECTION.IN).iterator();
      Assertions.assertFalse(vertices.hasNext());

      vertices = v2.getVertices(PVertex.DIRECTION.OUT).iterator();
      Assertions.assertTrue(vertices.hasNext());

      // Expecting 1 edge only: V2 is still connected to V3
      vertices = v3.getVertices(PVertex.DIRECTION.IN).iterator();
      Assertions.assertTrue(vertices.hasNext());
      vertices.next();
      Assertions.assertFalse(vertices.hasNext());

      // RELOAD AND CHECK AGAIN
      // -----------------------
      v2 = (PVertex) db.lookupByRID(v2.getIdentity(), true);

      vertices = v2.getVertices(PVertex.DIRECTION.IN).iterator();
      Assertions.assertFalse(vertices.hasNext());

      vertices = v2.getVertices(PVertex.DIRECTION.OUT).iterator();
      Assertions.assertTrue(vertices.hasNext());

      v3 = (PVertex) db.lookupByRID(v3.getIdentity(), true);

      // Expecting 1 edge only: V2 is still connected to V3
      vertices = v3.getVertices(PVertex.DIRECTION.IN).iterator();
      Assertions.assertTrue(vertices.hasNext());
      vertices.next();
      Assertions.assertFalse(vertices.hasNext());

      try {
        db.lookupByRID(root, true);
        Assertions.fail("Expected deleted record");
      } catch (PRecordNotFoundException e) {
      }

    } finally {
      new PDatabaseChecker().check(db);
      db.close();
    }
  }

  @Test
  public void deleteEdges() {
    final PDatabase db = new PDatabaseFactory(DB_PATH, PPaginatedFile.MODE.READ_WRITE).acquire();
    db.begin();
    try {

      PVertex v1 = (PVertex) db.lookupByRID(root, false);
      Assertions.assertNotNull(v1);

      Iterator<PEdge> edges = v1.getEdges(PVertex.DIRECTION.OUT).iterator();
      Assertions.assertTrue(edges.hasNext());
      PEdge e2 = edges.next();
      Assertions.assertNotNull(e2);

      Assertions.assertTrue(edges.hasNext());
      PEdge e3 = edges.next();
      Assertions.assertNotNull(e3);

      // DELETE THE EDGE
      // -----------------------
      db.deleteRecord(e2);

      PVertex vOut = e2.getOutVertex();
      edges = vOut.getEdges(PVertex.DIRECTION.OUT).iterator();
      Assertions.assertTrue(edges.hasNext());

      edges.next();
      Assertions.assertFalse(edges.hasNext());

      PVertex vIn = e2.getInVertex();
      edges = vIn.getEdges(PVertex.DIRECTION.IN).iterator();
      Assertions.assertFalse(edges.hasNext());

      // RELOAD AND CHECK AGAIN
      // -----------------------
      try {
        db.lookupByRID(e2.getIdentity(), true);
        Assertions.fail("Expected deleted record");
      } catch (PRecordNotFoundException e) {
      }

      vOut = e2.getOutVertex();
      edges = vOut.getEdges(PVertex.DIRECTION.OUT).iterator();
      Assertions.assertTrue(edges.hasNext());

      edges.next();
      Assertions.assertFalse(edges.hasNext());

      vIn = e2.getInVertex();
      edges = vIn.getEdges(PVertex.DIRECTION.IN).iterator();
      Assertions.assertFalse(edges.hasNext());

    } finally {
      new PDatabaseChecker().check(db);
      db.close();
    }
  }
}