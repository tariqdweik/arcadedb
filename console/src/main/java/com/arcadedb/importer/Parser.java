/*
 * Copyright (c) 2018 - Arcade Analytics LTD (https://arcadeanalytics.com)
 */

package com.arcadedb.importer;

import java.io.*;
import java.util.concurrent.atomic.AtomicLong;

public class Parser {
  private final InputStream       is;
  private final InputStreamReader reader;
  private final long              limit;
  private       AtomicLong        position = new AtomicLong();
  private       long              total;
  private       char              currentChar;
  private       boolean           compressed;

  public Parser(final Source source, final long limit) throws IOException {
    this.is = new BufferedInputStream(source.inputStream) {
      @Override
      public int read() throws IOException {
        position.incrementAndGet();
        return super.read();
      }

      @Override
      public int read(final byte[] b) throws IOException {
        if (limit > 0 && position.get() > limit)
          throw new EOFException();

        final int res = super.read(b);
        position.addAndGet(res);
        return res;
      }

      @Override
      public int read(final byte[] b, final int off, final int len) throws IOException {
        if (limit > 0 && position.get() > limit)
          throw new EOFException();

        int res = super.read(b, off, len);
        position.addAndGet(res);
        return res;
      }

      @Override
      public int available() throws IOException {
        if (limit > 0 && position.get() > limit)
          return 0;

        return super.available();
      }
    };

    this.compressed = source.compressed;
    this.total = source.totalSize;

    this.reader = new InputStreamReader(this.is);
    this.limit = limit;
    this.is.mark(0);
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
    is.mark(0);
  }

  public void reset() throws IOException {
    position.set(0);
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