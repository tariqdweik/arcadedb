/*
 * Copyright (c) 2018 - Arcade Analytics LTD (https://arcadeanalytics.com)
 */

package com.arcadedb.index;

import com.arcadedb.database.Identifiable;

import java.util.Collection;
import java.util.Iterator;

public class TempIndexCursor implements IndexCursor {
  private final Iterator<IndexCursorEntry> iterator;
  private final long                       size;
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
  public Identifiable getRecord() {
    return current.record;
  }

  @Override
  public boolean hasNext() {
    return iterator.hasNext();
  }

  @Override
  public Identifiable next() {
    current = iterator.next();
    return current.record;
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
  public long size() {
    return size;
  }

  @Override
  public Iterator<Identifiable> iterator() {
    return this;
  }
}
