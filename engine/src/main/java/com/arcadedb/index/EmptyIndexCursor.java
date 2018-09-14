/*
 * Copyright (c) 2018 - Arcade Analytics LTD (https://arcadeanalytics.com)
 */

package com.arcadedb.index;

import com.arcadedb.database.RID;

import java.util.Iterator;

public class EmptyIndexCursor implements IndexCursor {
  public EmptyIndexCursor() {
  }

  @Override
  public Object[] getKeys() {
    return null;
  }

  @Override
  public RID getRID() {
    return null;
  }

  @Override
  public boolean hasNext() {
    return false;
  }

  @Override
  public RID next() {
    return null;
  }

  @Override
  public int getScore() {
    return 0;
  }

  @Override
  public void close() {
  }

  @Override
  public String dumpStats() {
    return "no-stats";
  }

  @Override
  public int size() {
    return 0;
  }

  @Override
  public Iterator<RID> iterator() {
    return this;
  }
}
