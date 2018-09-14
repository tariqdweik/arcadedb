/*
 * Copyright (c) 2018 - Arcade Analytics LTD (https://arcadeanalytics.com)
 */

package com.arcadedb.index;

import com.arcadedb.database.RID;

import java.util.Collection;
import java.util.Iterator;

public class TempIndexCursor implements IndexCursor {
  private final Iterator<IndexCursorEntry> iterator;
  private final int                        size;
  private       IndexCursorEntry           current;

  public TempIndexCursor(final Collection<IndexCursorEntry> list) {
    this.iterator = list.iterator();
    this.size = list.size();
  }

  @Override
  public Object[] getKeys() {
    return current.keys;
  }

  @Override
  public RID getRID() {
    return current.rid;
  }

  @Override
  public boolean hasNext() {
    return iterator.hasNext();
  }

  @Override
  public RID next() {
    current = iterator.next();
    return current.rid;
  }

  @Override
  public int getScore() {
    return current.score;
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
    return size;
  }

  @Override
  public Iterator<RID> iterator() {
    return this;
  }
}
