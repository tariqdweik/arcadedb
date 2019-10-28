/*
 * Copyright (c) - Arcade Analytics LTD (https://arcadeanalytics.com)
 */
package com.arcadedb.database.async;

import com.arcadedb.database.Record;

public interface NewRecordCallback {
  void call(Record newRecord);
}
