package com.arcadedb.engine;

import com.arcadedb.database.PRID;

import java.io.IOException;
import java.util.List;

public interface PIndex {
  String getName();

  void compact() throws IOException;

  PIndexIterator newIterator(boolean ascendingOrder) throws IOException;

  PIndexIterator newIterator(boolean ascendingOrder, int startingPageId, int startingPosition) throws IOException;

  PIndexIterator newIterator(Object[] beginKeys, boolean beginInclusive, Object[] endKeys, boolean endInclusive);

  List<PRID> get(Object[] keys);

  void put(Object[] keys, PRID rid);
}
