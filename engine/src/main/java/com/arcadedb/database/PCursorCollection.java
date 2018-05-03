package com.arcadedb.database;

import java.util.Collection;
import java.util.Iterator;

public class PCursorCollection<T extends PIdentifiable> implements PCursor<T> {
  private final Collection<T> collection;
  private final Iterator<T>   iterator;

  public PCursorCollection(final Collection<T> collection) {
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
