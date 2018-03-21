package com.arcadedb.graph;

import com.arcadedb.database.PIdentifiable;
import com.arcadedb.database.PModifiableDocument;
import com.arcadedb.database.PRID;
import com.arcadedb.engine.*;
import com.arcadedb.index.PIndex;
import com.arcadedb.index.PIndexCursor;
import com.arcadedb.schema.PDocumentType;
import com.arcadedb.schema.PEdgeType;
import com.arcadedb.schema.PSchemaImpl;

import java.io.IOException;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

public class PGraph {
  public static final PRID NULL_RID = new PRID(null, -1, -1);

  public static void newEdge(final PVertex vertex, final String edgeType, final PIdentifiable toVertex,
      final boolean bidirectional) {
    if (toVertex == null)
      throw new IllegalArgumentException("Destination vertex is null");

    final PRID rid = vertex.getIdentity();
    if (rid == null)
      throw new IllegalArgumentException("Current vertex is not persistent");

    if (toVertex instanceof PModifiableDocument && toVertex.getIdentity() == null)
      throw new IllegalArgumentException("Target vertex is not persistent");

    final PEdgeType type = PGraph.getEdgeType(vertex, edgeType);

    final PIndex edgeIndex = vertex.getDatabase().getSchema().getIndexByName(PSchemaImpl.EDGES_INDEX_NAME);

    final Object[] outKeys = new Object[] { rid, (byte) PVertex.DIRECTION.OUT.ordinal(), type.getDictionaryId(), toVertex };

    // DON'T CREATE THE EDGE UNTIL IT'S NEEDED
    edgeIndex.put(outKeys, NULL_RID);

    if (bidirectional) {
      final Object[] inKeys = new Object[] { toVertex.getIdentity(), (byte) PVertex.DIRECTION.IN.ordinal(), type.getDictionaryId(),
          rid };

      // DON'T CREATE THE EDGE UNTIL IT'S NEEDED
      edgeIndex.put(inKeys, NULL_RID);
    }
  }

  public static Iterator<PEdge> getVertexEdges(final PVertex vertex, final PVertex.DIRECTION direction, final String edgeType) {
    if (direction == null)
      throw new IllegalArgumentException("Direction is null");

    final PEdgeType type = getEdgeType(vertex, edgeType);

    return Collections.EMPTY_LIST.iterator();
  }

  /**
   * Returns all the connected vertices, both directions, any edge type.
   *
   * @return An iterator of PIndexCursorEntry entries
   */
  public static Iterator<PGraphCursorEntry> getVertexConnectedVertices(final PVertex vertex) {
    // TODO implement lazy fetching
    final PIndex edgeIndex = vertex.getDatabase().getSchema().getIndexByName(PSchemaImpl.EDGES_INDEX_NAME);

    final PDictionary dictionary = vertex.getDatabase().getSchema().getDictionary();

    final Set<PGraphCursorEntry> result = new HashSet<>();
    try {
      final Object[] keys = new Object[] { vertex.getIdentity() };
      final PIndexCursor outVertices = edgeIndex.iterator(keys);

      while (outVertices.hasNext()) {
        outVertices.next();
        final Object[] entryKeys = outVertices.getKeys();
        result.add(new PGraphLSMCursorEntry((PIdentifiable) entryKeys[0], (PVertex.DIRECTION) entryKeys[1],
            dictionary.getNameById((Integer) entryKeys[2]), (PIdentifiable) entryKeys[3], (PIdentifiable) outVertices.getValue()));
      }

    } catch (IOException e) {
      throw new RuntimeException("Error on browsing outgoing vertices for vertex " + vertex.getIdentity(), e);
    }

    return result.iterator();
  }

  /**
   * Returns the connected vertices.
   *
   * @param direction Direction between OUT, IN or BOTH
   * @param edgeType  Edge type name to filter
   *
   * @return An iterator of PIndexCursorEntry entries
   */
  public static Iterator<PGraphCursorEntry> getVertexConnectedVertices(final PVertex vertex, final PVertex.DIRECTION direction,
      final String edgeType) {
    if (direction == null)
      throw new IllegalArgumentException("Direction is null");

    final PEdgeType type = getEdgeType(vertex, edgeType);

    final PIndex edgeIndex = vertex.getDatabase().getSchema().getIndexByName(PSchemaImpl.EDGES_INDEX_NAME);

    final PDictionary dictionary = vertex.getDatabase().getSchema().getDictionary();

    final Set<PGraphCursorEntry> result = new HashSet<>();
    if (direction == PVertex.DIRECTION.OUT || direction == PVertex.DIRECTION.BOTH) {
      try {
        final Object[] keys = new Object[] { vertex.getIdentity(), (byte) PVertex.DIRECTION.OUT.ordinal(), type.getDictionaryId() };
        final PIndexCursor outVertices = edgeIndex.iterator(keys);

        while (outVertices.hasNext()) {
          outVertices.next();
          final Object[] entryKeys = outVertices.getKeys();
          result.add(new PGraphLSMCursorEntry((PIdentifiable) entryKeys[0], PVertex.DIRECTION.values()[(Byte) entryKeys[1]],
              dictionary.getNameById((Integer) entryKeys[2]), (PIdentifiable) entryKeys[3],
              (PIdentifiable) outVertices.getValue()));
        }

      } catch (IOException e) {
        throw new RuntimeException("Error on browsing outgoing vertices for vertex " + vertex.getIdentity(), e);
      }
    } else if (direction == PVertex.DIRECTION.IN || direction == PVertex.DIRECTION.BOTH) {
      try {
        final Object[] keys = new Object[] { vertex.getIdentity(), (byte) PVertex.DIRECTION.IN.ordinal(), type.getDictionaryId() };
        final PIndexCursor inVertices = edgeIndex.iterator(keys);

        while (inVertices.hasNext()) {
          inVertices.next();
          final Object[] entryKeys = inVertices.getKeys();
          result.add(new PGraphLSMCursorEntry((PIdentifiable) entryKeys[0], PVertex.DIRECTION.values()[(Byte) entryKeys[1]],
              dictionary.getNameById((Integer) entryKeys[2]), (PIdentifiable) entryKeys[3], (PIdentifiable) inVertices.getValue()));
        }

      } catch (IOException e) {
        throw new RuntimeException("Error on browsing incoming vertices for vertex " + vertex.getIdentity(), e);
      }
    }

    return result.iterator();
  }

  /**
   * Returns the connected vertices.
   *
   * @param direction Direction between OUT, IN or BOTH
   *
   * @return An iterator of PIndexCursorEntry entries
   */
  public static Iterator<PGraphCursorEntry> getVertexConnectedVertices(final PVertex vertex, final PVertex.DIRECTION direction) {
    if (direction == null)
      throw new IllegalArgumentException("Direction is null");

    final PIndex edgeIndex = vertex.getDatabase().getSchema().getIndexByName(PSchemaImpl.EDGES_INDEX_NAME);

    final PDictionary dictionary = vertex.getDatabase().getSchema().getDictionary();

    final Set<PGraphCursorEntry> result = new HashSet<>();
    if (direction == PVertex.DIRECTION.OUT || direction == PVertex.DIRECTION.BOTH) {
      try {
        final Object[] keys = new Object[] { vertex.getIdentity(), (byte) PVertex.DIRECTION.OUT.ordinal() };
        final PIndexCursor outVertices = edgeIndex.iterator(keys);

        while (outVertices.hasNext()) {
          outVertices.next();
          final Object[] entryKeys = outVertices.getKeys();
          result.add(new PGraphLSMCursorEntry((PIdentifiable) entryKeys[0], PVertex.DIRECTION.values()[(Byte) entryKeys[1]],
              dictionary.getNameById((Integer) entryKeys[2]), (PIdentifiable) entryKeys[3],
              (PIdentifiable) outVertices.getValue()));
        }

      } catch (IOException e) {
        throw new RuntimeException("Error on browsing outgoing vertices for vertex " + vertex.getIdentity(), e);
      }
    } else if (direction == PVertex.DIRECTION.IN || direction == PVertex.DIRECTION.BOTH) {
      try {
        final Object[] keys = new Object[] { vertex.getIdentity(), (byte) PVertex.DIRECTION.IN.ordinal() };
        final PIndexCursor inVertices = edgeIndex.iterator(keys);

        while (inVertices.hasNext()) {
          inVertices.next();
          final Object[] entryKeys = inVertices.getKeys();
          result.add(new PGraphLSMCursorEntry((PIdentifiable) entryKeys[0], PVertex.DIRECTION.values()[(Byte) entryKeys[1]],
              dictionary.getNameById((Integer) entryKeys[2]), (PIdentifiable) entryKeys[3], (PIdentifiable) inVertices.getValue()));
        }

      } catch (IOException e) {
        throw new RuntimeException("Error on browsing incoming vertices for vertex " + vertex.getIdentity(), e);
      }
    }

    return result.iterator();
  }

  public static boolean isVertexConnectedTo(final PVertex vertex, final PIdentifiable toVertex) {
    if (toVertex == null)
      throw new IllegalArgumentException("toVertex is null");

    final PIndex edgeIndex = vertex.getDatabase().getSchema().getIndexByName(PSchemaImpl.EDGES_INDEX_NAME);

    try {
      final Object[] keys = new Object[] { vertex.getIdentity() };
      final PIndexCursor outVertices = edgeIndex.iterator(keys);

      while (outVertices.hasNext()) {
        outVertices.next();
        final Object[] entryKeys = outVertices.getKeys();
        if (((PIdentifiable) entryKeys[3]).getIdentity().equals(toVertex))
          return true;
      }

    } catch (IOException e) {
      throw new RuntimeException("Error on checking if vertices are connected between vertex " + vertex.getIdentity(), e);
    }
    return false;
  }

  public static boolean isVertexConnectedTo(final PVertex vertex, final PIdentifiable toVertex, final PVertex.DIRECTION direction) {
    if (toVertex == null)
      throw new IllegalArgumentException("toVertex is null");

    if (direction == null)
      throw new IllegalArgumentException("Direction is null");

    final PIndex edgeIndex = vertex.getDatabase().getSchema().getIndexByName(PSchemaImpl.EDGES_INDEX_NAME);

    try {
      final Object[] keys = new Object[] { vertex.getIdentity(), (byte) direction.ordinal() };
      final PIndexCursor outVertices = edgeIndex.iterator(keys);

      while (outVertices.hasNext()) {
        outVertices.next();
        final Object[] entryKeys = outVertices.getKeys();
        if (((PIdentifiable) entryKeys[3]).getIdentity().equals(toVertex))
          return true;
      }

    } catch (IOException e) {
      throw new RuntimeException("Error on checking if vertices are connected between vertex " + vertex.getIdentity(), e);
    }
    return false;
  }

  public static boolean isVertexConnectedTo(final PVertex vertex, final PIdentifiable toVertex, final PVertex.DIRECTION direction,
      final String edgeType) {
    if (toVertex == null)
      throw new IllegalArgumentException("toVertex is null");

    if (direction == null)
      throw new IllegalArgumentException("Direction is null");

    final PEdgeType type = getEdgeType(vertex, edgeType);

    final PIndex edgeIndex = vertex.getDatabase().getSchema().getIndexByName(PSchemaImpl.EDGES_INDEX_NAME);

    try {
      final Object[] keys = new Object[] { vertex.getIdentity(), (byte) direction.ordinal(), type.getDictionaryId(), toVertex };
      final PIndexCursor outVertices = edgeIndex.iterator(keys);

      return outVertices.hasNext();

    } catch (IOException e) {
      throw new RuntimeException("Error on checking if vertices are connected between vertex " + vertex.getIdentity(), e);
    }
  }

  public static PEdgeType getEdgeType(final PVertex vertex, final String edgeType) {
    if (edgeType == null)
      throw new IllegalArgumentException("Edge type is null");

    final PDocumentType type = vertex.getDatabase().getSchema().getType(edgeType);
    if (!(type instanceof PEdgeType))
      throw new IllegalArgumentException("Type '" + edgeType + "' is not an edge type");

    return (PEdgeType) type;
  }
}
