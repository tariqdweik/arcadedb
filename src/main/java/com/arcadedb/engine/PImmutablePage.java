package com.arcadedb.engine;

/**
 * Low level immutable (read-only) page implementation of 65536 bytes (2 exp 16 = 65Kb). The first 8 bytes (the header) are reserved
 * to store he page version (MVCC). The maximum content is 65528.
 */
public class PImmutablePage extends PBasePage {

  public PImmutablePage(final PPageManager manager, final PPageId pageId, final int size) {
    this(manager, pageId, size, new byte[size], 0, 0);
  }

  public PImmutablePage(final PPageManager manager, final PPageId pageId, final int size, final byte[] content, final int version,
      final int contentSize) {
    super(manager, pageId, size, content, version, contentSize);
  }
}
