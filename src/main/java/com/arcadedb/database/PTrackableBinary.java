package com.arcadedb.database;

import com.arcadedb.engine.PTrackableContent;

import java.nio.ByteBuffer;

public class PTrackableBinary extends PBinary implements PTrackableContent {
  private final PTrackableContent derivedFrom;

  public PTrackableBinary(final PTrackableContent derivedFrom, final ByteBuffer slice) {
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

  public PBinary slice() {
    return new PTrackableBinary(this, buffer.slice());
  }

  public PBinary slice(final int position) {
    buffer.position(position);
    return new PTrackableBinary(this, buffer.slice());
  }

  public PBinary slice(final int position, final int length) {
    buffer.position(position);
    final ByteBuffer result = buffer.slice();
    result.position(length);
    result.flip();
    return new PTrackableBinary(this, result);
  }
}
