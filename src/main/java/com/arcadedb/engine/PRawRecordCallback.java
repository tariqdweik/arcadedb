package com.arcadedb.engine;

import com.arcadedb.database.PBinary;
import com.arcadedb.database.PRID;

public interface PRawRecordCallback {
  boolean onRecord(PRID rid, PBinary view);
}
