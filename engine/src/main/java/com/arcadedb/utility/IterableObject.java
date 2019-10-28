/*
 * Copyright (c) - Arcade Analytics LTD (https://arcadeanalytics.com)
 */

package com.arcadedb.utility;

import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 * Allows to iterate over a single object
 **/
public class IterableObject<T> implements Iterable<T>, Iterator<T> {

  private final T object;
  private boolean alreadyRead = false;

  public IterableObject(T o) {
    object = o;
  }

  /**
   * Returns an iterator over a set of elements of type T.
   *
   * @return an Iterator.
   */
  public Iterator<T> iterator() {
    return this;
  }

  @Override
  public boolean hasNext() {
    return !alreadyRead;
  }

  @Override
  public T next() {
    if (!alreadyRead) {
      alreadyRead = true;
      return object;
    } else
      throw new NoSuchElementException();
  }

  @Override
  public void remove() {
    throw new UnsupportedOperationException("remove");
  }
}
