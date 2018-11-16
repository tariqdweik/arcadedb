/*
 * Copyright (c) 2018 - Arcade Analytics LTD (https://arcadeanalytics.com)
 */

package com.arcadedb.index;

import com.arcadedb.database.RID;

import java.util.Iterator;

public interface IndexCursor extends Iterable<RID>, Iterator<RID> {
  Object[] getKeys();

  RID getRID();

  int getScore();

  void close();

  String dumpStats();

  int size();
}
