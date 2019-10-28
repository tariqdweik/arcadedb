/*
 * Copyright (c) - Arcade Analytics LTD (https://arcadeanalytics.com)
 */
package com.arcadedb.utility;

/**
 * Container for pair of non null objects.
 */
public class Pair<V1, V2> {
  private final V1 first;
  private final V2 second;

  public Pair(final V1 first, final V2 second) {
    this.first = first;
    this.second = second;
  }

  public V1 getFirst() {
    return first;
  }

  public V2 getSecond() {
    return second;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o)
      return true;
    if (o == null || getClass() != o.getClass())
      return false;

    final Pair<?, ?> oRawPair = (Pair<?, ?>) o;

    if (!first.equals(oRawPair.first))
      return false;
    return second.equals(oRawPair.second);
  }

  @Override
  public int hashCode() {
    int result = first.hashCode();
    result = 31 * result + second.hashCode();
    return result;
  }

  @Override
  public String toString() {
    return "<" + first + "," + second + ">";
  }
}
