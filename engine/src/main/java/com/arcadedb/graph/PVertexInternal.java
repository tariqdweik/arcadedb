package com.arcadedb.graph;

import com.arcadedb.database.PRID;

public interface PVertexInternal extends PVertex {
  PRID getOutEdgesHeadChunk();

  PRID getInEdgesHeadChunk();

  void setOutEdgesHeadChunk(PRID newChunk);

  void setInEdgesHeadChunk(PRID newChunk);
}
