package com.arcadedb.database;

import com.arcadedb.engine.PIndex;
import com.arcadedb.engine.PIndexIterator;
import com.arcadedb.schema.PDocumentType;
import com.arcadedb.schema.PEdgeType;

import java.io.IOException;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

public class PVertex extends PModifiableDocument {
  public static final byte RECORD_TYPE = 1;
  public static final PRID NULL_RID    = new PRID(-1, -1);

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

  public Iterator<PVertex> getVertices(final DIRECTION direction, final String edgeType) {
    if (direction == null)
      throw new IllegalArgumentException("Direction is null");

    final PEdgeType type = getEdgeType(edgeType);

    final PIndex edgeIndex = database.getSchema().getIndexByName(typeName + "_edges");

    final Set<PVertex> result = new HashSet<>();
    if (direction == DIRECTION.OUT || direction == DIRECTION.BOTH) {
      try {
        final Object[] keys = new Object[] { getIdentity(), (byte) DIRECTION.OUT.ordinal(), type.getDictionaryId() };
        final PIndexIterator outVertices = edgeIndex.iterator(keys);

        while (outVertices.hasNext()) {
          outVertices.next();
          result.add((PVertex) database.lookupByRID((PRID) outVertices.getKeys()[3]));
        }

      } catch (IOException e) {
        throw new RuntimeException("Error on browsing outgoing vertices for vertex " + getIdentity(), e);
      }
    } else if (direction == DIRECTION.IN || direction == DIRECTION.BOTH) {
      try {
        final Object[] keys = new Object[] { getIdentity(), (byte) DIRECTION.IN.ordinal(), type.getDictionaryId() };
        final PIndexIterator inVertices = edgeIndex.iterator(keys);

        while (inVertices.hasNext()) {
          inVertices.next();
          result.add((PVertex) database.lookupByRID((PRID) inVertices.getKeys()[3]));
        }

      } catch (IOException e) {
        throw new RuntimeException("Error on browsing incoming vertices for vertex " + getIdentity(), e);
      }

    }

    return result.iterator();
  }

  public void isConnectedTo(final DIRECTION direction, final String edgeType, final PIdentifiable toVertex) {
    // TODO
    throw new UnsupportedOperationException();
  }

  public void newEdge(final String edgeType, final PIdentifiable toVertex, final boolean bidirectional) {
    if (toVertex == null)
      throw new IllegalArgumentException("Destination vertex is null");

    if (rid == null)
      throw new IllegalArgumentException("Current vertex is not persistent");

    if (toVertex instanceof PModifiableDocument && toVertex.getIdentity() == null)
      throw new IllegalArgumentException("Target vertex is not persistent");

    final PEdgeType type = getEdgeType(edgeType);

    final PIndex edgeIndex = database.getSchema().getIndexByName(typeName + "_edges");

    final Object[] outKeys = new Object[] { getIdentity(), (byte) DIRECTION.OUT.ordinal(), ((PEdgeType) type).getDictionaryId(),
        toVertex };

    // DON'T CREATE THE EDGE UNTIL IT'S NEEDED
    edgeIndex.put(outKeys, NULL_RID);

    if (bidirectional) {
      final Object[] inKeys = new Object[] { toVertex.getIdentity(), (byte) DIRECTION.IN.ordinal(),
          ((PEdgeType) type).getDictionaryId(), getIdentity() };

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
