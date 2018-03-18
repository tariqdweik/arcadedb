package com.arcadedb.engine;

import com.arcadedb.database.PRID;

import java.io.IOException;
import java.util.List;

public interface PIndex {
  String getName();

  void compact() throws IOException;

  PIndexCursor iterator(Object[] fromKeys) throws IOException;

  PIndexCursor iterator(boolean ascendingOrder) throws IOException;

  PIndexCursor iterator(boolean ascendingOrder, Object[] fromKeys) throws IOException;

  PIndexCursor range(Object[] beginKeys, Object[] endKeys) throws IOException;

  List<PRID> get(Object[] keys);

  void put(Object[] keys, PRID rid);
}
