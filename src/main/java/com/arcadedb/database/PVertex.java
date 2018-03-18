package com.arcadedb.database;

import com.arcadedb.engine.*;
import com.arcadedb.schema.PDocumentType;
import com.arcadedb.schema.PEdgeType;
import com.arcadedb.schema.PSchemaImpl;

import java.io.IOException;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

public class PVertex extends PModifiableDocument {
  public static final byte RECORD_TYPE = 1;
  public static final PRID NULL_RID    = new PRID(null, -1, -1);

  public enum DIRECTION {
    OUT, IN, BOTH
  }

  public PVertex(final PDatabase graph, final String typeName, final PRID rid) {
    super(graph, typeName, rid);
  }

  public PVertex(final PDatabase graph, final String typeName, final PRID rid, final PBinary buffer) {
    super(graph, typeName, rid, buffer);
  }

  public Iterator<PEdge> getEdges(final DIRECTION direction, final String edgeType) {
    if (direction == null)
      throw new IllegalArgumentException("Direction is null");

    final PEdgeType type = getEdgeType(edgeType);

    return Collections.EMPTY_LIST.iterator();
  }

  /**
   * Returns all the connected vertices, both directions, any edge type.
   *
   * @return An iterator of PIndexCursorEntry entries
   */
  public Iterator<PIndexCursorEntry> getConnectedVertices() {
    // TODO implement lazy fetching
    final PIndex edgeIndex = database.getSchema().getIndexByName(PSchemaImpl.EDGES_INDEX_NAME);

    final PDictionary dictionary = database.getSchema().getDictionary();

    final Set<PIndexCursorEntry> result = new HashSet<>();
    try {
      final Object[] keys = new Object[] { getIdentity() };
      final PIndexCursor outVertices = edgeIndex.iterator(keys);

      while (outVertices.hasNext()) {
        outVertices.next();
        final Object[] entryKeys = outVertices.getKeys();
        result.add(new PIndexLSMCursorEntry((PIdentifiable) entryKeys[0], (DIRECTION) entryKeys[1],
            dictionary.getNameById((Integer) entryKeys[2]), (PIdentifiable) entryKeys[3], (PIdentifiable) outVertices.getValue()));
      }

    } catch (IOException e) {
      throw new RuntimeException("Error on browsing outgoing vertices for vertex " + getIdentity(), e);
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
  public Iterator<PIndexCursorEntry> getConnectedVertices(final DIRECTION direction, final String edgeType) {
    if (direction == null)
      throw new IllegalArgumentException("Direction is null");

    final PEdgeType type = getEdgeType(edgeType);

    final PIndex edgeIndex = database.getSchema().getIndexByName(PSchemaImpl.EDGES_INDEX_NAME);

    final PDictionary dictionary = database.getSchema().getDictionary();

    final Set<PIndexCursorEntry> result = new HashSet<>();
    if (direction == DIRECTION.OUT || direction == DIRECTION.BOTH) {
      try {
        final Object[] keys = new Object[] { getIdentity(), (byte) DIRECTION.OUT.ordinal(), type.getDictionaryId() };
        final PIndexCursor outVertices = edgeIndex.iterator(keys);

        while (outVertices.hasNext()) {
          outVertices.next();
          final Object[] entryKeys = outVertices.getKeys();
          result.add(new PIndexLSMCursorEntry((PIdentifiable) entryKeys[0], DIRECTION.values()[(Byte) entryKeys[1]],
              dictionary.getNameById((Integer) entryKeys[2]), (PIdentifiable) entryKeys[3],
              (PIdentifiable) outVertices.getValue()));
        }

      } catch (IOException e) {
        throw new RuntimeException("Error on browsing outgoing vertices for vertex " + getIdentity(), e);
      }
    } else if (direction == DIRECTION.IN || direction == DIRECTION.BOTH) {
      try {
        final Object[] keys = new Object[] { getIdentity(), (byte) DIRECTION.IN.ordinal(), type.getDictionaryId() };
        final PIndexCursor inVertices = edgeIndex.iterator(keys);

        while (inVertices.hasNext()) {
          inVertices.next();
          final Object[] entryKeys = inVertices.getKeys();
          result.add(new PIndexLSMCursorEntry((PIdentifiable) entryKeys[0], DIRECTION.values()[(Byte) entryKeys[1]],
              dictionary.getNameById((Integer) entryKeys[2]), (PIdentifiable) entryKeys[3], (PIdentifiable) inVertices.getValue()));
        }

      } catch (IOException e) {
        throw new RuntimeException("Error on browsing incoming vertices for vertex " + getIdentity(), e);
      }
    }

    return result.iterator();
  }

  public boolean isConnectedTo(final PIdentifiable toVertex) {
    if (toVertex == null)
      throw new IllegalArgumentException("toVertex is null");

    final PIndex edgeIndex = database.getSchema().getIndexByName(PSchemaImpl.EDGES_INDEX_NAME);

    try {
      final Object[] keys = new Object[] { getIdentity() };
      final PIndexCursor outVertices = edgeIndex.iterator(keys);

      while (outVertices.hasNext()) {
        outVertices.next();
        final Object[] entryKeys = outVertices.getKeys();
        if (((PIdentifiable) entryKeys[3]).getIdentity().equals(toVertex))
          return true;
      }

    } catch (IOException e) {
      throw new RuntimeException("Error on checking if vertices are connected between vertex " + getIdentity(), e);
    }
    return false;
  }

  public boolean isConnectedTo(final PIdentifiable toVertex, final DIRECTION direction) {
    if (toVertex == null)
      throw new IllegalArgumentException("toVertex is null");

    if (direction == null)
      throw new IllegalArgumentException("Direction is null");

    final PIndex edgeIndex = database.getSchema().getIndexByName(PSchemaImpl.EDGES_INDEX_NAME);

    try {
      final Object[] keys = new Object[] { getIdentity(), (byte) direction.ordinal() };
      final PIndexCursor outVertices = edgeIndex.iterator(keys);

      while (outVertices.hasNext()) {
        outVertices.next();
        final Object[] entryKeys = outVertices.getKeys();
        if (((PIdentifiable) entryKeys[3]).getIdentity().equals(toVertex))
          return true;
      }

    } catch (IOException e) {
      throw new RuntimeException("Error on checking if vertices are connected between vertex " + getIdentity(), e);
    }
    return false;
  }

  public boolean isConnectedTo(final PIdentifiable toVertex, final DIRECTION direction, final String edgeType) {
    if (toVertex == null)
      throw new IllegalArgumentException("toVertex is null");

    if (direction == null)
      throw new IllegalArgumentException("Direction is null");

    final PEdgeType type = getEdgeType(edgeType);

    final PIndex edgeIndex = database.getSchema().getIndexByName(PSchemaImpl.EDGES_INDEX_NAME);

    try {
      final Object[] keys = new Object[] { getIdentity(), (byte) direction.ordinal(), type.getDictionaryId(), toVertex };
      final PIndexCursor outVertices = edgeIndex.iterator(keys);

      return outVertices.hasNext();

    } catch (IOException e) {
      throw new RuntimeException("Error on checking if vertices are connected between vertex " + getIdentity(), e);
    }
  }

  public void newEdge(final String edgeType, final PIdentifiable toVertex, final boolean bidirectional) {
    if (toVertex == null)
      throw new IllegalArgumentException("Destination vertex is null");

    if (rid == null)
      throw new IllegalArgumentException("Current vertex is not persistent");

    if (toVertex instanceof PModifiableDocument && toVertex.getIdentity() == null)
      throw new IllegalArgumentException("Target vertex is not persistent");

    final PEdgeType type = getEdgeType(edgeType);

    final PIndex edgeIndex = database.getSchema().getIndexByName(PSchemaImpl.EDGES_INDEX_NAME);

    final Object[] outKeys = new Object[] { getIdentity(), (byte) DIRECTION.OUT.ordinal(), type.getDictionaryId(), toVertex };

    // DON'T CREATE THE EDGE UNTIL IT'S NEEDED
    edgeIndex.put(outKeys, NULL_RID);

    if (bidirectional) {
      final Object[] inKeys = new Object[] { toVertex.getIdentity(), (byte) DIRECTION.IN.ordinal(), type.getDictionaryId(),
          getIdentity() };

      // DON'T CREATE THE EDGE UNTIL IT'S NEEDED
      edgeIndex.put(inKeys, NULL_RID);
    }
  }

  @Override
  public byte getRecordType() {
    return RECORD_TYPE;
  }

  private PEdgeType getEdgeType(final String edgeType) {
    if (edgeType == null)
      throw new IllegalArgumentException("Edge type is null");

    final PDocumentType type = database.getSchema().getType(edgeType);
    if (!(type instanceof PEdgeType))
      throw new IllegalArgumentException("Type '" + typeName + "' is not an edge type");

    return (PEdgeType) type;
  }
}
