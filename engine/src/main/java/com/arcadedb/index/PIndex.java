/*
 * Copyright (c) 2018 - Arcade Analytics LTD (https://arcadeanalytics.com)
 */

package com.arcadedb.index;

import com.arcadedb.database.RID;

import java.io.IOException;
import java.util.List;
import java.util.Map;

public interface PIndex {
  String getName();

  void compact() throws IOException;

  PIndexCursor iterator(Object[] fromKeys) throws IOException;

  PIndexCursor iterator(boolean ascendingOrder) throws IOException;

  PIndexCursor iterator(boolean ascendingOrder, Object[] fromKeys) throws IOException;

  PIndexCursor range(Object[] beginKeys, Object[] endKeys) throws IOException;

  List<RID> get(Object[] keys);

  void put(Object[] keys, RID rid);

  void remove(Object[] keys);

  Map<String, Long> getStats();

  int getFileId();
}
