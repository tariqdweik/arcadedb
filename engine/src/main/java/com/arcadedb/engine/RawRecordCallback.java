/*
 * Copyright (c) 2018 - Arcade Analytics LTD (https://arcadeanalytics.com)
 */

package com.arcadedb.engine;

import com.arcadedb.database.Binary;
import com.arcadedb.database.RID;

public interface RawRecordCallback {
  boolean onRecord(RID rid, Binary view);
}
