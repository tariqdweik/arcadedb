/*
 * Copyright (c) - Arcade Data LTD (https://arcadedata.com)
 */
package com.arcadedb.database.async;

import com.arcadedb.database.Record;

public interface NewRecordCallback {
  void call(Record newRecord);
}
