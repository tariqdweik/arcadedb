/*
 * Copyright (c) - Arcade Data LTD (https://arcadedata.com)
 */

package com.arcadedb.graph;

import com.arcadedb.database.RID;

/**
 * Not intended to be used by the end-user. Internal only.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 * @see Vertex
 */
public interface VertexInternal extends Vertex {
  RID getOutEdgesHeadChunk();

  RID getInEdgesHeadChunk();

  void setOutEdgesHeadChunk(RID newChunk);

  void setInEdgesHeadChunk(RID newChunk);
}
