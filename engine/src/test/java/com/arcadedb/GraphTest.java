/*
 * Copyright (c) 2018 - Arcade Analytics LTD (https://arcadeanalytics.com)
 */

package com.arcadedb;

import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.engine.DatabaseChecker;
import com.arcadedb.engine.PaginatedFile;
import com.arcadedb.exception.RecordNotFoundException;
import com.arcadedb.graph.Edge;
import com.arcadedb.graph.ModifiableEdge;
import com.arcadedb.graph.ModifiableVertex;
import com.arcadedb.graph.Vertex;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Iterator;

public class GraphTest extends BaseGraphTest {

  @Test
  public void checkVertices() {
    final Database db2 = new DatabaseFactory(DB_PATH, PaginatedFile.MODE.READ_ONLY).open();
    db2.begin();
    try {

      Assertions.assertEquals(1, db2.countType(VERTEX1_TYPE_NAME, false));
      Assertions.assertEquals(2, db2.countType(VERTEX2_TYPE_NAME, false));

      final Vertex v1 = (Vertex) db2.lookupByRID(root, false);
      Assertions.assertNotNull(v1);

      // TEST CONNECTED VERTICES
      Assertions.assertEquals(VERTEX1_TYPE_NAME, v1.getType());
      Assertions.assertEquals(VERTEX1_TYPE_NAME, v1.get("name"));

      final Iterator<Vertex> vertices2level = v1.getVertices(Vertex.DIRECTION.OUT, new String[] { EDGE1_TYPE_NAME }).iterator();
      Assertions.assertNotNull(vertices2level);
      Assertions.assertTrue(vertices2level.hasNext());

      final Vertex v2 = vertices2level.next();

      Assertions.assertNotNull(v2);
      Assertions.assertEquals(VERTEX2_TYPE_NAME, v2.getType());
      Assertions.assertEquals(VERTEX2_TYPE_NAME, v2.get("name"));

      final Iterator<Vertex> vertices2level2 = v1.getVertices(Vertex.DIRECTION.OUT, new String[] { EDGE2_TYPE_NAME }).iterator();
      Assertions.assertTrue(vertices2level2.hasNext());

      final Vertex v3 = vertices2level2.next();
      Assertions.assertNotNull(v3);

      Assertions.assertEquals(VERTEX2_TYPE_NAME, v3.getType());
      Assertions.assertEquals("V3", v3.get("name"));

      final Iterator<Vertex> vertices3level = v2.getVertices(Vertex.DIRECTION.OUT, new String[] { EDGE2_TYPE_NAME }).iterator();
      Assertions.assertNotNull(vertices3level);
      Assertions.assertTrue(vertices3level.hasNext());

      final Vertex v32 = vertices3level.next();

      Assertions.assertNotNull(v32);
      Assertions.assertEquals(VERTEX2_TYPE_NAME, v32.getType());
      Assertions.assertEquals("V3", v32.get("name"));

      Assertions.assertTrue(v1.isConnectedTo(v2));
      Assertions.assertTrue(v2.isConnectedTo(v1));
      Assertions.assertTrue(v1.isConnectedTo(v3));
      Assertions.assertTrue(v3.isConnectedTo(v1));
      Assertions.assertTrue(v2.isConnectedTo(v3));

      Assertions.assertFalse(v3.isConnectedTo(v1, Vertex.DIRECTION.OUT));
      Assertions.assertFalse(v3.isConnectedTo(v2, Vertex.DIRECTION.OUT));

    } finally {
      db2.close();
    }
  }

  @Test
  public void checkEdges() {
    final Database db2 = new DatabaseFactory(DB_PATH, PaginatedFile.MODE.READ_ONLY).open();
    db2.begin();
    try {

      Assertions.assertEquals(1, db2.countType(EDGE1_TYPE_NAME, false));
      Assertions.assertEquals(2, db2.countType(EDGE2_TYPE_NAME, false));

      final Vertex v1 = (Vertex) db2.lookupByRID(root, false);
      Assertions.assertNotNull(v1);

      // TEST CONNECTED EDGES
      final Iterator<Edge> edges1 = v1.getEdges(Vertex.DIRECTION.OUT, new String[] { EDGE1_TYPE_NAME }).iterator();
      Assertions.assertNotNull(edges1);
      Assertions.assertTrue(edges1.hasNext());

      final Edge e1 = edges1.next();

      Assertions.assertNotNull(e1);
      Assertions.assertEquals(EDGE1_TYPE_NAME, e1.getType());
      Assertions.assertEquals(v1, e1.getOut());
      Assertions.assertEquals("E1", e1.get("name"));

      Vertex v2 = e1.getInVertex();
      Assertions.assertEquals(VERTEX2_TYPE_NAME, v2.get("name"));

      final Iterator<Edge> edges2 = v2.getEdges(Vertex.DIRECTION.OUT, new String[] { EDGE2_TYPE_NAME }).iterator();
      Assertions.assertTrue(edges2.hasNext());

      final Edge e2 = edges2.next();
      Assertions.assertNotNull(e2);

      Assertions.assertEquals(EDGE2_TYPE_NAME, e2.getType());
      Assertions.assertEquals(v2, e2.getOut());
      Assertions.assertEquals("E2", e2.get("name"));

      Vertex v3 = e2.getInVertex();
      Assertions.assertEquals("V3", v3.get("name"));

      final Iterator<Edge> edges3 = v1.getEdges(Vertex.DIRECTION.OUT, new String[] { EDGE2_TYPE_NAME }).iterator();
      Assertions.assertNotNull(edges3);
      Assertions.assertTrue(edges3.hasNext());

      final Edge e3 = edges3.next();

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
    final Database db = new DatabaseFactory(DB_PATH, PaginatedFile.MODE.READ_WRITE).open();
    db.begin();
    try {

      Assertions.assertEquals(1, db.countType(EDGE1_TYPE_NAME, false));
      Assertions.assertEquals(2, db.countType(EDGE2_TYPE_NAME, false));

      final Vertex v1 = (Vertex) db.lookupByRID(root, false);
      Assertions.assertNotNull(v1);

      final ModifiableVertex v1Copy = (ModifiableVertex) v1.modify();
      v1Copy.set("newProperty1", "TestUpdate1");
      v1Copy.save();

      // TEST CONNECTED EDGES
      final Iterator<Edge> edges1 = v1.getEdges(Vertex.DIRECTION.OUT, new String[] { EDGE1_TYPE_NAME }).iterator();
      Assertions.assertNotNull(edges1);
      Assertions.assertTrue(edges1.hasNext());

      final Edge e1 = edges1.next();

      Assertions.assertNotNull(e1);

      final ModifiableEdge e1Copy = (ModifiableEdge) e1.modify();
      e1Copy.set("newProperty2", "TestUpdate2");
      e1Copy.save();

      db.commit();

      final Vertex v1CopyReloaded = (Vertex) db.lookupByRID(v1Copy.getIdentity(), true);
      Assertions.assertEquals("TestUpdate1", v1CopyReloaded.get("newProperty1"));
      final Edge e1CopyReloaded = (Edge) db.lookupByRID(e1Copy.getIdentity(), true);
      Assertions.assertEquals("TestUpdate2", e1CopyReloaded.get("newProperty2"));

    } finally {
      new DatabaseChecker().check(db);
      db.close();
    }
  }

  @Test
  public void deleteVertices() {
    final Database db = new DatabaseFactory(DB_PATH, PaginatedFile.MODE.READ_WRITE).open();
    db.begin();
    try {

      Vertex v1 = (Vertex) db.lookupByRID(root, false);
      Assertions.assertNotNull(v1);

      Iterator<Vertex> vertices = v1.getVertices(Vertex.DIRECTION.OUT).iterator();
      Assertions.assertTrue(vertices.hasNext());
      Vertex v2 = vertices.next();
      Assertions.assertNotNull(v2);

      Assertions.assertTrue(vertices.hasNext());
      Vertex v3 = vertices.next();
      Assertions.assertNotNull(v3);

      final long totalVertices = db.countType(v1.getType(), true);

      // DELETE THE VERTEX
      // -----------------------
      db.deleteRecord(v1);

      Assertions.assertEquals(totalVertices - 1, db.countType(v1.getType(), true));

      vertices = v2.getVertices(Vertex.DIRECTION.IN).iterator();
      Assertions.assertFalse(vertices.hasNext());

      vertices = v2.getVertices(Vertex.DIRECTION.OUT).iterator();
      Assertions.assertTrue(vertices.hasNext());

      // Expecting 1 edge only: V2 is still connected to V3
      vertices = v3.getVertices(Vertex.DIRECTION.IN).iterator();
      Assertions.assertTrue(vertices.hasNext());
      vertices.next();
      Assertions.assertFalse(vertices.hasNext());

      // RELOAD AND CHECK AGAIN
      // -----------------------
      v2 = (Vertex) db.lookupByRID(v2.getIdentity(), true);

      vertices = v2.getVertices(Vertex.DIRECTION.IN).iterator();
      Assertions.assertFalse(vertices.hasNext());

      vertices = v2.getVertices(Vertex.DIRECTION.OUT).iterator();
      Assertions.assertTrue(vertices.hasNext());

      v3 = (Vertex) db.lookupByRID(v3.getIdentity(), true);

      // Expecting 1 edge only: V2 is still connected to V3
      vertices = v3.getVertices(Vertex.DIRECTION.IN).iterator();
      Assertions.assertTrue(vertices.hasNext());
      vertices.next();
      Assertions.assertFalse(vertices.hasNext());

      try {
        db.lookupByRID(root, true);
        Assertions.fail("Expected deleted record");
      } catch (RecordNotFoundException e) {
      }

    } finally {
      new DatabaseChecker().check(db);
      db.close();
    }
  }

  @Test
  public void deleteEdges() {
    final Database db = new DatabaseFactory(DB_PATH, PaginatedFile.MODE.READ_WRITE).open();
    db.begin();
    try {

      Vertex v1 = (Vertex) db.lookupByRID(root, false);
      Assertions.assertNotNull(v1);

      Iterator<Edge> edges = v1.getEdges(Vertex.DIRECTION.OUT).iterator();
      Assertions.assertTrue(edges.hasNext());
      Edge e2 = edges.next();
      Assertions.assertNotNull(e2);

      Assertions.assertTrue(edges.hasNext());
      Edge e3 = edges.next();
      Assertions.assertNotNull(e3);

      // DELETE THE EDGE
      // -----------------------
      db.deleteRecord(e2);

      Vertex vOut = e2.getOutVertex();
      edges = vOut.getEdges(Vertex.DIRECTION.OUT).iterator();
      Assertions.assertTrue(edges.hasNext());

      edges.next();
      Assertions.assertFalse(edges.hasNext());

      Vertex vIn = e2.getInVertex();
      edges = vIn.getEdges(Vertex.DIRECTION.IN).iterator();
      Assertions.assertFalse(edges.hasNext());

      // RELOAD AND CHECK AGAIN
      // -----------------------
      try {
        db.lookupByRID(e2.getIdentity(), true);
        Assertions.fail("Expected deleted record");
      } catch (RecordNotFoundException e) {
      }

      vOut = e2.getOutVertex();
      edges = vOut.getEdges(Vertex.DIRECTION.OUT).iterator();
      Assertions.assertTrue(edges.hasNext());

      edges.next();
      Assertions.assertFalse(edges.hasNext());

      vIn = e2.getInVertex();
      edges = vIn.getEdges(Vertex.DIRECTION.IN).iterator();
      Assertions.assertFalse(edges.hasNext());

    } finally {
      new DatabaseChecker().check(db);
      db.close();
    }
  }

  @Test
  public void selfLoopEdges() {
    final Database db = new DatabaseFactory(DB_PATH, PaginatedFile.MODE.READ_WRITE).open();
    db.begin();
    try {

      // UNIDIRECTIONAL EDGE
      final Vertex v1 = db.newVertex(VERTEX1_TYPE_NAME).save();
      v1.newEdge(EDGE1_TYPE_NAME, v1, false).save();

      Assertions.assertTrue(v1.getVertices(Vertex.DIRECTION.OUT).iterator().hasNext());
      Assertions.assertEquals(v1, v1.getVertices(Vertex.DIRECTION.OUT).iterator().next());
      Assertions.assertFalse(v1.getVertices(Vertex.DIRECTION.IN).iterator().hasNext());

      // BIDIRECTIONAL EDGE
      final Vertex v2 = db.newVertex(VERTEX1_TYPE_NAME).save();
      v2.newEdge(EDGE1_TYPE_NAME, v2, true).save();

      Assertions.assertTrue(v2.getVertices(Vertex.DIRECTION.OUT).iterator().hasNext());
      Assertions.assertEquals(v2, v2.getVertices(Vertex.DIRECTION.OUT).iterator().next());

      Assertions.assertTrue(v2.getVertices(Vertex.DIRECTION.IN).iterator().hasNext());
      Assertions.assertEquals(v2, v2.getVertices(Vertex.DIRECTION.IN).iterator().next());

      db.commit();

      // UNIDIRECTIONAL EDGE
      final Vertex v1reloaded = (Vertex) db.lookupByRID(v1.getIdentity(), true);
      Assertions.assertTrue(v1reloaded.getVertices(Vertex.DIRECTION.OUT).iterator().hasNext());
      Assertions.assertEquals(v1reloaded, v1reloaded.getVertices(Vertex.DIRECTION.OUT).iterator().next());
      Assertions.assertFalse(v1reloaded.getVertices(Vertex.DIRECTION.IN).iterator().hasNext());

      // BIDIRECTIONAL EDGE
      final Vertex v2reloaded = (Vertex) db.lookupByRID(v2.getIdentity(), true);

      Assertions.assertTrue(v2reloaded.getVertices(Vertex.DIRECTION.OUT).iterator().hasNext());
      Assertions.assertEquals(v2reloaded, v2reloaded.getVertices(Vertex.DIRECTION.OUT).iterator().next());

      Assertions.assertTrue(v2reloaded.getVertices(Vertex.DIRECTION.IN).iterator().hasNext());
      Assertions.assertEquals(v2reloaded, v2reloaded.getVertices(Vertex.DIRECTION.IN).iterator().next());

    } finally {
      new DatabaseChecker().check(db);
      db.close();
    }
  }

}