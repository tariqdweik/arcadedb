/*
 * Copyright (c) 2018 - Arcade Analytics LTD (https://arcadeanalytics.com)
 */
package com.arcadedb.utility;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * The most simpler LRU cache implementation in Java.
 */
public class LRUCache<K, V> extends LinkedHashMap<K, V> {

  private static final long serialVersionUID = 0;

  final private int cacheSize;

  public LRUCache(final int iCacheSize) {
    super(16, (float) 0.75, true);
    this.cacheSize = iCacheSize;
  }

  protected boolean removeEldestEntry(final Map.Entry<K, V> eldest) {
    return size() >= cacheSize;
  }
}
