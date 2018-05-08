package com.arcadedb.engine;

import com.arcadedb.database.Binary;
import com.arcadedb.database.RID;

public interface RawRecordCallback {
  boolean onRecord(RID rid, Binary view);
}
