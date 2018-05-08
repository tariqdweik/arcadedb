package com.arcadedb.engine;

import com.arcadedb.database.Binary;
import com.arcadedb.database.TrackableBinary;

/**
 * Low level modifiable page implementation. The first 8 bytes (the header) are reserved to store the page version (MVCC).
 */
public class ModifiablePage extends BasePage implements TrackableContent {
  private int     modifiedRangeFrom = Integer.MAX_VALUE;
  private int     modifiedRangeTo   = -1;
  private WALFile walFile;

  public ModifiablePage(final PageManager manager, final PageId pageId, final int size) {
    this(manager, pageId, size, new byte[size], 0, 0);
    updateModifiedRange(0, size - 1);
  }

  public ModifiablePage(final PageManager manager, final PageId pageId, final int size, final byte[] array, final int version,
      final int contentSize) {
    super(manager, pageId, size, array, version, contentSize);
  }

  public TrackableBinary getTrackable() {
    content.getByteBuffer().position(PAGE_HEADER_SIZE);
    return new TrackableBinary(this, content.getByteBuffer().slice());
  }

  public void incrementVersion() {
    updateModifiedRange(0, Binary.LONG_SERIALIZED_SIZE);
    version++;
  }

  public int writeNumber(int index, final long content) {
    index += PAGE_HEADER_SIZE;
    checkBoundariesOnWrite(index, Binary.LONG_SERIALIZED_SIZE + 1); // WITH VARSIZE NUMBER THE WORST CASE SCENARIO IS 1 BYTE MORE
    return this.content.putNumber(index, content);
  }

  public void writeLong(int index, final long content) {
    index += PAGE_HEADER_SIZE;
    checkBoundariesOnWrite(index, Binary.LONG_SERIALIZED_SIZE);
    this.content.putLong(index, content);
  }

  public void writeInt(int index, final int content) {
    index += PAGE_HEADER_SIZE;
    checkBoundariesOnWrite(index, Binary.INT_SERIALIZED_SIZE);
    this.content.putInt(index, content);
  }

  public void writeUnsignedInt(int index, final long content) {
    index += PAGE_HEADER_SIZE;
    checkBoundariesOnWrite(index, Binary.INT_SERIALIZED_SIZE);
    this.content.putInt(index, (int) content);
  }

  public void writeShort(int index, final short content) {
    index += PAGE_HEADER_SIZE;
    checkBoundariesOnWrite(index, Binary.SHORT_SERIALIZED_SIZE);
    this.content.putShort(index, content);
  }

  public void writeUnsignedShort(int index, final int content) {
    index += PAGE_HEADER_SIZE;
    checkBoundariesOnWrite(index, Binary.SHORT_SERIALIZED_SIZE);
    this.content.putShort(index, (short) content);
  }

  public void writeByte(int index, final byte content) {
    index += PAGE_HEADER_SIZE;
    checkBoundariesOnWrite(index, Binary.BYTE_SERIALIZED_SIZE);
    this.content.putByte(index, content);
  }

  public int writeBytes(int index, final byte[] content) {
    index += PAGE_HEADER_SIZE;
    checkBoundariesOnWrite(index, content.length);
    return this.content.putBytes(index, content);
  }

  public void writeByteArray(int index, final byte[] content) {
    index += PAGE_HEADER_SIZE;
    checkBoundariesOnWrite(index, content.length);
    this.content.putByteArray(index, content);
  }

  public int writeString(final int index, final String content) {
    return writeBytes(index, content.getBytes());
  }

  public void blank(final int index, final int length) {
    checkBoundariesOnWrite(PAGE_HEADER_SIZE + index, length);
    content.position(PAGE_HEADER_SIZE + index + length);
  }

  public int getAvailableContentSize() {
    return getPhysicalSize() - getContentSize();
  }

  @Override
  public int[] getModifiedRange() {
    return new int[] { modifiedRangeFrom, modifiedRangeTo };
  }

  @Override
  public void updateModifiedRange(final int start, final int end) {
    if (start < modifiedRangeFrom)
      modifiedRangeFrom = start;
    if (end > modifiedRangeTo)
      modifiedRangeTo = end;
  }

  public WALFile getWALFile() {
    return walFile;
  }

  public void setWALFile(final WALFile WALFile) {
    this.walFile = WALFile;
  }

  private void checkBoundariesOnWrite(final int start, final int length) {
    if (start < 0)
      throw new IllegalArgumentException("Invalid position " + start);

    if (start + length > getPhysicalSize())
      throw new IllegalArgumentException(
          "Cannot write outside the page space (" + (start + length) + ">" + getPhysicalSize() + ")");

    updateModifiedRange(start, start + length - 1);
  }
}
