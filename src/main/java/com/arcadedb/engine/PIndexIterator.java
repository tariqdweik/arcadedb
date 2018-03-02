package com.arcadedb.engine;

import com.arcadedb.database.PBinary;
import com.arcadedb.serializer.PBinarySerializer;

import java.io.IOException;

import static com.arcadedb.database.PBinary.INT_SERIALIZED_SIZE;

public class PIndexIterator {
  private final PIndex            index;
  private final PBinary           buffer;
  private final byte[]            keyTypes;
  private final int               keyStartPosition;
  private final PBinarySerializer serializer;
  private final int               totalKeys;

  private int currentKeyIndex = 0;
  private int valuePosition   = -1;
  private Object[] nextKeys;
  private Object   nextValue;

  public PIndexIterator(final PIndex index, final PBinary buffer, final int keyStartPosition, final byte[] keyTypes,
      final int totalKeys) {
    this.index = index;
    this.buffer = buffer;
    this.keyStartPosition = keyStartPosition;
    this.keyTypes = keyTypes;
    this.serializer = index.database.getSerializer();
    this.totalKeys = totalKeys;
  }

  public boolean hasNext() throws IOException {
    return currentKeyIndex < totalKeys - 1;
  }

  public void next() throws IOException {
    ++currentKeyIndex;
    nextKeys = null;
    nextValue = null;
  }

  public Object[] getKeys() {
    if (nextKeys != null)
      return nextKeys;

    final int contentPos = buffer.getInt(keyStartPosition + (currentKeyIndex * INT_SERIALIZED_SIZE));
    buffer.position(contentPos);

    nextKeys = new Object[keyTypes.length];
    for (int k = 0; k < keyTypes.length; ++k)
      nextKeys[k] = index.database.getSerializer().deserializeValue(buffer, keyTypes[k]);

    valuePosition = buffer.position();
    nextValue = null;

    return nextKeys;
  }

  public Object getValue() {
    if (nextValue == null) {
      if (valuePosition < 0)
        getKeys();
      nextValue = index.getValue(buffer, serializer, valuePosition);
    }
    return nextValue;
  }
}
