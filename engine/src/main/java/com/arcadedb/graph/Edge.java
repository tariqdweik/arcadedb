/*
 * Copyright (c) - Arcade Data LTD (https://arcadedata.com)
 */

package com.arcadedb.graph;

import com.arcadedb.database.Document;
import com.arcadedb.database.RID;

/**
 * An Edge represents the connection between two vertices in a Property Graph. The edge can have properties and point to the same vertex.
 * The direction of the edge goes from the source vertex to the destination vertex. By default edges are bidirectional, that means they can be traversed from
 * both sides. Unidirectional edges can only be traversed from the direction they were created, never backwards. Edges can be Immutable (read-only) and Mutable.
 *
 * @author Luca Garulli (l.garulli@arcadedata.it)
 * @see Vertex
 */
public interface Edge extends Document {
  byte RECORD_TYPE = 2;

  MutableEdge modify();

  RID getOut();

  Vertex getOutVertex();

  RID getIn();

  Vertex getInVertex();

  Vertex getVertex(Vertex.DIRECTION iDirection);
}
