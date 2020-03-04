/*
 * Copyright (c) - Arcade Data LTD (https://arcadedata.com)
 */

package com.arcadedb.index;

import com.arcadedb.database.Identifiable;
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
  public RID getRecord() {
    return null;
  }

  @Override
  public boolean hasNext() {
    return false;
  }

  @Override
  public Identifiable next() {
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
  public long size() {
    return 0l;
  }

  @Override
  public Iterator<Identifiable> iterator() {
    return this;
  }
}
