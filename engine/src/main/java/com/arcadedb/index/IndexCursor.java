/*
 * Copyright (c) 2018 - Arcade Analytics LTD (https://arcadeanalytics.com)
 */

package com.arcadedb.index;

import com.arcadedb.database.Identifiable;

import java.util.Iterator;

public interface IndexCursor extends Iterable<Identifiable>, Iterator<Identifiable> {
  Object[] getKeys();

  Identifiable getRecord();

  int getScore();

  void close();

  String dumpStats();

  long size();
}
