/*
 * Copyright (c) 2018 - Arcade Analytics LTD (https://arcadeanalytics.com)
 */

package com.arcadedb.analyzer;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.concurrent.atomic.AtomicLong;

public class Parser {
  private final InputStream       is;
  private final InputStreamReader reader;
  private final long              limit;
  private       AtomicLong        position = new AtomicLong();
  private       long              total;
  private       char              currentChar;
  private       boolean           compressed;

  public Parser(final InputStream is, final long total, final long limit, final boolean compressed) throws IOException {
    final BufferedInputStream wrapped = new BufferedInputStream(is) {
      @Override
      public int read() throws IOException {
        position.incrementAndGet();
        return super.read();
      }

      @Override
      public int read(final byte[] b) throws IOException {
        if (limit > 0 && position.get() > limit)
          return 0;

        final int res = super.read(b);
        position.addAndGet(res);
        return res;
      }

      @Override
      public int read(final byte[] b, final int off, final int len) throws IOException {
        if (limit > 0 && position.get() > limit)
          return 0;

        int res = super.read(b, off, len);
        position.addAndGet(res);
        return res;
      }

      @Override
      public int available() throws IOException {
        if (limit > 0 && position.get() > limit)
          return 0;

        return is.available();
      }
    };

    this.is = new BufferedInputStream(wrapped);
    this.reader = new InputStreamReader(this.is);
    this.total = total;
    this.limit = limit;
    this.compressed = compressed;
    this.is.mark(0);

    if (this.is.available() > 0)
      this.currentChar = (char) this.reader.read();
  }

  public char getCurrentChar() {
    return currentChar;
  }

  public char nextChar() throws IOException {
    position.incrementAndGet();
    currentChar = (char) reader.read();
    return currentChar;
  }

  public void mark() {
    is.mark((int) position.get());
  }

  public void reset() throws IOException {
    is.reset();
  }

  public boolean isAvailable() throws IOException {
    if (limit > 0)
      return position.get() < limit && is.available() > 0;
    return is.available() > 0;
  }

  public InputStream getInputStream() {
    return is;
  }

  public long getPosition() {
    return position.get();
  }

  public long getTotal() {
    return limit > 0 ? Math.min(limit, total) : total;
  }

  public boolean isCompressed() {
    return compressed;
  }
}