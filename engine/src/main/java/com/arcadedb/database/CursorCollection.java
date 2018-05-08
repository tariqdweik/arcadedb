/*
 * Copyright (c) 2018 - Arcade Analytics LTD (https://arcadeanalytics.com)
 */

package com.arcadedb.database;

import java.util.Collection;
import java.util.Iterator;

public class CursorCollection<T extends Identifiable> implements Cursor<T> {
  private final Collection<T> collection;
  private final Iterator<T>   iterator;

  public CursorCollection(final Collection<T> collection) {
    this.collection = collection;
    this.iterator = collection.iterator();
  }

  public long size() {
    return collection.size();
  }

  @Override
  public Iterator<T> iterator() {
    return iterator;
  }

  @Override
  public boolean hasNext() {
    return iterator.hasNext();
  }

  @Override
  public T next() {
    return iterator.next();
  }
}
