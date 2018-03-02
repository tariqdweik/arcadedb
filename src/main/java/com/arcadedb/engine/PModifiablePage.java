package com.arcadedb.engine;

import com.arcadedb.database.PBinary;

/**
 * Low level modifiable page implementation of 65536 bytes by default (2 exp 16 = 65Kb). The first 8 bytes (the header) are reserved
 * to store the page version (MVCC).
 */
public class PModifiablePage extends PBasePage {
  protected int dirtyFrom = -1;// AS USHORT
  protected int dirtyTo   = -1;// AS USHORT

  public PModifiablePage(final PPageManager manager, final PPageId pageId, final int size) {
    this(manager, pageId, size, new byte[size], 0, 0);
  }

  public PModifiablePage(final PPageManager manager, final PPageId pageId, final int size, final byte[] array, final int version,
      final int contentSize) {
    super(manager, pageId, size, array, version, contentSize);
  }

  public void incrementVersion() {
    version++;
  }

  public boolean isDirty() {
    return dirtyFrom > -1;
  }

  public void writeNumber(int index, final long content) {
    index += PAGE_HEADER_SIZE;
    checkBoundariesOnWrite(index, PBinary.LONG_SERIALIZED_SIZE);
    this.content.putLong(index, content);
  }

  public void writeLong(int index, final long content) {
    index += PAGE_HEADER_SIZE;
    checkBoundariesOnWrite(index, PBinary.LONG_SERIALIZED_SIZE);
    this.content.putLong(index, content);
  }

  public void writeInt(int index, final int content) {
    index += PAGE_HEADER_SIZE;
    checkBoundariesOnWrite(index, PBinary.INT_SERIALIZED_SIZE);
    this.content.putInt(index, content);
  }

  public void writeUnsignedInt(int index, final long content) {
    index += PAGE_HEADER_SIZE;
    checkBoundariesOnWrite(index, PBinary.INT_SERIALIZED_SIZE);
    this.content.putInt(index, (int) content);
  }

  public void writeShort(int index, final short content) {
    index += PAGE_HEADER_SIZE;
    checkBoundariesOnWrite(index, PBinary.SHORT_SERIALIZED_SIZE);
    this.content.putShort(index, content);
  }

  public void writeUnsignedShort(int index, final int content) {
    index += PAGE_HEADER_SIZE;
    checkBoundariesOnWrite(index, PBinary.SHORT_SERIALIZED_SIZE);
    this.content.putShort(index, (short) content);
  }

  public void writeByte(int index, final byte content) {
    index += PAGE_HEADER_SIZE;
    checkBoundariesOnWrite(index, PBinary.BYTE_SERIALIZED_SIZE);
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

  public void unsetDirty() {
    this.dirtyFrom = this.dirtyTo = -1;
  }

  /**
   * Creates an immutable copy where the content has been copied too.
   */
  public PImmutablePage createImmutableCopy() {
    final byte[] contentCopy = new byte[getPhysicalSize()];
    System.arraycopy(content.getByteBuffer().array(), 0, contentCopy, 0, content.size());
    return new PImmutablePage(manager, pageId, getPhysicalSize(), contentCopy, version, content.size());
  }

  public int[] getDirtyBoundaries() {
    return new int[] { dirtyFrom, dirtyTo };
  }

  public void blank(final int index, final int length) {
    checkBoundariesOnWrite(PAGE_HEADER_SIZE + index, length);
    content.position(PAGE_HEADER_SIZE + index + length);
  }

  public int getAvailableContentSize() {
    return getPhysicalSize() - getContentSize();
  }

  private void checkBoundariesOnWrite(final int start, final int length) {
    if (start < 0 || start + length > getPhysicalSize())
      throw new IllegalArgumentException(
          "Cannot write outside the page space (" + (start + length) + ">" + getPhysicalSize() + ")");

    if (this.dirtyFrom == -1 || start < dirtyFrom)
      this.dirtyFrom = start;
    if (this.dirtyTo == -1 || start + length > this.dirtyTo)
      this.dirtyTo = start + length;

  }
}
