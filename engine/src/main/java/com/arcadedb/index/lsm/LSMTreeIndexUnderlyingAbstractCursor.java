/*
 * Copyright (c) - Arcade Analytics LTD (https://arcadeanalytics.com)
 */

package com.arcadedb.index.lsm;

import com.arcadedb.database.RID;
import com.arcadedb.engine.PageId;
import com.arcadedb.serializer.BinarySerializer;

public abstract class LSMTreeIndexUnderlyingAbstractCursor {
  protected final LSMTreeIndexAbstract index;
  protected final byte[]               keyTypes;
  protected final BinarySerializer     serializer;
  protected final int                  totalKeys;
  protected final boolean              ascendingOrder;

  public LSMTreeIndexUnderlyingAbstractCursor(final LSMTreeIndexAbstract index, final byte[] keyTypes, final int totalKeys, final boolean ascendingOrder) {
    this.index = index;
    this.keyTypes = keyTypes;
    this.serializer = index.getDatabase().getSerializer();
    this.totalKeys = totalKeys;
    this.ascendingOrder = ascendingOrder;
  }

  public abstract boolean hasNext();

  public abstract void next();

  public abstract Object[] getKeys();

  public abstract RID[] getValue();

  public abstract PageId getCurrentPageId();

  public abstract int getCurrentPositionInPage();

  public void close() {
  }
}
