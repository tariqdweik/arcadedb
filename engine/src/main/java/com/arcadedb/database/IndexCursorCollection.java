/*
 * Copyright (c) - Arcade Data LTD (https://arcadedata.com)
 */

package com.arcadedb.database;

import com.arcadedb.index.IndexCursor;

import java.util.Collection;
import java.util.Iterator;

public class IndexCursorCollection implements IndexCursor {
  private final Collection<Identifiable> collection;
  private final Iterator<Identifiable>   iterator;
  private       Identifiable             last = null;

  public IndexCursorCollection(final Collection<Identifiable> collection) {
    this.collection = collection;
    this.iterator = collection.iterator();
  }

  @Override
  public Object[] getKeys() {
    return new Object[0];
  }

  @Override
  public Identifiable getRecord() {
    return last;
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

  public long size() {
    return collection.size();
  }

  @Override
  public Iterator<Identifiable> iterator() {
    return iterator;
  }

  @Override
  public boolean hasNext() {
    return iterator.hasNext();
  }

  @Override
  public Identifiable next() {
    last = iterator.next();
    return last;
  }
}
