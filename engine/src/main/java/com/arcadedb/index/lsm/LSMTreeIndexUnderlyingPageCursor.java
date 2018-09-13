/*
 * Copyright (c) 2018 - Arcade Analytics LTD (https://arcadeanalytics.com)
 */

package com.arcadedb.index.lsm;

import com.arcadedb.database.Binary;
import com.arcadedb.engine.BasePage;
import com.arcadedb.engine.PageId;

import static com.arcadedb.database.Binary.INT_SERIALIZED_SIZE;

public class LSMTreeIndexUnderlyingPageCursor extends LSMTreeIndexUnderlyingAbstractCursor {
  protected final PageId pageId;
  protected final Binary buffer;
  protected final int    keyStartPosition;

  protected int      currentEntryIndex;
  protected int      valuePosition = -1;
  protected Object[] nextKeys;
  protected Object[] nextValue;

  public LSMTreeIndexUnderlyingPageCursor(final LSMTreeIndexAbstract index, final BasePage page, final int currentEntryInPage, final int keyStartPosition,
      final byte[] keyTypes, final int totalKeys, final boolean ascendingOrder) {
    super(index, keyTypes, totalKeys, ascendingOrder);

    this.keyStartPosition = keyStartPosition;
    this.pageId = page.getPageId();
    this.buffer = new Binary(page.slice());
    this.currentEntryIndex = currentEntryInPage;
  }

  public boolean hasNext() {
    if (ascendingOrder)
      return currentEntryIndex < totalKeys - 1;
    return currentEntryIndex > 0;
  }

  public void next() {
    currentEntryIndex += ascendingOrder ? 1 : -1;
    nextKeys = null;
    nextValue = null;
  }

  public Object[] getKeys() {
    if (nextKeys != null)
      return nextKeys;

    if (currentEntryIndex < 0)
      throw new IllegalStateException("Invalid page cursor index " + currentEntryIndex);

    final int contentPos = buffer.getInt(keyStartPosition + (currentEntryIndex * INT_SERIALIZED_SIZE));
    buffer.position(contentPos);

    nextKeys = new Object[keyTypes.length];
    for (int k = 0; k < keyTypes.length; ++k)
      nextKeys[k] = index.getDatabase().getSerializer().deserializeValue(index.getDatabase(), buffer, keyTypes[k]);

    valuePosition = buffer.position();
    nextValue = index.readEntryValues(buffer);

    return nextKeys;
  }

  public Object[] getValue() {
    if (nextValue == null) {
      if (valuePosition < 0)
        getKeys();
      buffer.position(valuePosition);
      nextValue = index.readEntryValues(buffer);
    }
    return nextValue;
  }

  @Override
  public PageId getCurrentPageId() {
    return pageId;
  }

  @Override
  public int getCurrentPositionInPage() {
    return currentEntryIndex;
  }
}
