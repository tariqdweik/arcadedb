/*
 * Copyright (c) - Arcade Analytics LTD (https://arcadeanalytics.com)
 */

package com.arcadedb.importer;

import com.arcadedb.log.LogManager;

import java.io.InputStream;
import java.util.concurrent.Callable;
import java.util.logging.Level;

public class Source {
  public final  String         url;
  public final  InputStream    inputStream;
  public final  long           totalSize;
  public final  boolean        compressed;
  private final Callable<Void> closeCallback;

  public Source(final String url, final InputStream inputStream, final long totalSize, final boolean compressed, final Callable<Void> closeCallback) {
    this.url = url;
    this.inputStream = inputStream;
    this.totalSize = totalSize;
    this.compressed = compressed;
    this.closeCallback = closeCallback;
  }

  public void close() {
    try {
      closeCallback.call();
    } catch (Exception e) {
      LogManager.instance().log(this, Level.SEVERE, "Error on closing source %s", e, this);
    }
  }

  @Override
  public String toString() {
    return url + " (compressed=" + compressed + " size=" + totalSize + ")";
  }
}