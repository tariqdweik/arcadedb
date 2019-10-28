/*
 * Copyright (c) - Arcade Analytics LTD (https://arcadeanalytics.com)
 */

package com.arcadedb.database;

import com.arcadedb.engine.TrackableContent;

import java.nio.ByteBuffer;

public class TrackableBinary extends Binary implements TrackableContent {
  private final TrackableContent derivedFrom;

  public TrackableBinary(final TrackableContent derivedFrom, final ByteBuffer slice) {
    super(slice);
    this.derivedFrom = derivedFrom;
  }

  public int[] getModifiedRange() {
    return derivedFrom.getModifiedRange();
  }

  public void updateModifiedRange(final int start, final int end) {
    derivedFrom.updateModifiedRange(buffer.arrayOffset() + start, buffer.arrayOffset() + end);
  }

  @Override
  protected void checkForAllocation(final int offset, final int bytesToWrite) {
    super.checkForAllocation(offset, bytesToWrite);
    updateModifiedRange(offset, offset + bytesToWrite - 1);
  }

  public void move(final int startPosition, final int destPosition, final int length) {
    super.move(startPosition, destPosition, length);
    updateModifiedRange(startPosition, destPosition + length);
  }

  public Binary slice() {
    return new TrackableBinary(this, buffer.slice());
  }

  public Binary slice(final int position) {
    buffer.position(position);
    return new TrackableBinary(this, buffer.slice());
  }

  public Binary slice(final int position, final int length) {
    buffer.position(position);
    final ByteBuffer result = buffer.slice();
    result.position(length);
    result.flip();
    return new TrackableBinary(this, result);
  }
}
