/*
 * Copyright (c) - Arcade Data LTD (https://arcadedata.com)
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
