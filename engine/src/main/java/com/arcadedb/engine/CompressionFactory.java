/*
 * Copyright (c) 2018 - Arcade Analytics LTD (https://arcadeanalytics.com)
 */

package com.arcadedb.engine;

/**
 * Compression factory.
 */
public class CompressionFactory {
  private static final LZ4Compression defaultImplementation = new LZ4Compression();

  public static Compression getDefault() {
    return defaultImplementation;
  }
}
