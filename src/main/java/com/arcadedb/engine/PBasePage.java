package com.arcadedb.engine;

import com.arcadedb.database.PBinary;

import java.nio.ByteBuffer;

/**
 * Low level base page implementation of (default) 65536 bytes (2 exp 16 = 65Kb). The first 4 bytes (the header) are reserved to
 * store he page version (MVCC), then 4 bytes more for the actual page content size. Content size is stored in PBinary object. The
 * maximum content for a page is pageSize - 16.
 */
public abstract class PBasePage {
  protected static final int PAGE_VERSION_OFFSET     = 0;
  protected static final int PAGE_CONTENTSIZE_OFFSET = PBinary.INT_SERIALIZED_SIZE;
  protected static final int PAGE_HEADER_SIZE        = PBinary.INT_SERIALIZED_SIZE + PBinary.INT_SERIALIZED_SIZE;

  protected final PPageManager manager;

  protected final PPageId pageId;
  protected final PBinary content;
  private final   int     size;
  protected       int     version;
  private long lastAccessed = System.currentTimeMillis();

  protected PBasePage(final PPageManager manager, final PPageId pageId, final int size, final byte[] buffer, final int version,
      final int contentSize) {
    this.manager = manager;
    this.pageId = pageId;
    this.size = size;
    this.content = new PBinary(buffer, contentSize);
    this.version = version;
  }

  public PModifiablePage createModifiableCopy() {
    final PModifiablePage copy = new PModifiablePage(manager, pageId, size, this.content.getByteBuffer().array(), version,
        content.size());
    // COPY THE CONTENT, SO CHANGES DOES NOT AFFECT IMMUTABLE COPY
    copy.version = version;
    return copy;
  }

  public void loadMetadata() {
    version = content.getInt(PAGE_VERSION_OFFSET);
    content.size(content.getInt(PAGE_CONTENTSIZE_OFFSET));
  }

  public void flushMetadata() {
    content.putInt(PAGE_VERSION_OFFSET, version);
    content.putInt(PAGE_CONTENTSIZE_OFFSET, content.size());
  }

  public int getPhysicalSize() {
    return size;
  }

  public int getMaxContentSize() {
    return getPhysicalSize() - PAGE_HEADER_SIZE;
  }

  public int getAvailableContentSize() {
    return getPhysicalSize() - getContentSize();
  }

  public int getContentSize() {
    return content.size() - PAGE_HEADER_SIZE;
  }

  public long getVersion() {
    return version;
  }

  public long readNumber(final int index) {
    return this.content.getNumber(PAGE_HEADER_SIZE + index);
  }

  public long[] readNumberAndSize(final int index) {
    return this.content.getNumberAndSize(PAGE_HEADER_SIZE + index);
  }

  public long readLong(final int index) {
    return this.content.getLong(PAGE_HEADER_SIZE + index);
  }

  public int readInt(final int index) {
    return this.content.getInt(PAGE_HEADER_SIZE + index);
  }

  public long readUnsignedInt(final int index) {
    return (long) this.content.getInt(PAGE_HEADER_SIZE + index) & 0xffffffffl;
  }

  public short readShort(final int index) {
    return this.content.getShort(PAGE_HEADER_SIZE + index);
  }

  public int readUnsignedShort(final int index) {
    return (int) this.content.getShort(PAGE_HEADER_SIZE + index) & 0xffff;
  }

  public byte readByte(final int index) {
    return this.content.getByte(PAGE_HEADER_SIZE + index);
  }

  public int readUnsignedByte(final int index) {
    return (int) this.content.getByte(PAGE_HEADER_SIZE + index) & 0xFF;
  }

  public void readByteArray(final int index, final byte[] buffer) {
    this.content.getByteArray(PAGE_HEADER_SIZE + index, buffer);
  }

  public byte[] readBytes(final int index) {
    return this.content.getBytes(PAGE_HEADER_SIZE + index);
  }

  public byte[] readBytes() {
    return this.content.getBytes();
  }

  public String readString() {
    return new String(readBytes());
  }

  public String readString(final int index) {
    return new String(readBytes(PAGE_HEADER_SIZE + index));
  }

  /**
   * Creates a copy of the ByteBuffer without copying the array[].
   *
   * @param index The starting position to copy
   */
  public PBinary getImmutableView(final int index, final int length) {
    content.position(index + PAGE_HEADER_SIZE);
    final ByteBuffer view = content.slice();
    view.position(length);
    view.flip();
    return new PBinary(view);
  }

  public PPageId getPageId() {
    return pageId;
  }

  public ByteBuffer getContent() {
    return content.getByteBuffer();
  }

  public ByteBuffer slice() {
    content.getByteBuffer().position(PAGE_HEADER_SIZE);
    return content.getByteBuffer().slice();
  }

  public long getLastAccessed() {
    return lastAccessed;
  }

  public void updateLastAccesses() {
    lastAccessed = System.currentTimeMillis();
  }

  public int getBufferPosition() {
    return this.content.position() - PAGE_HEADER_SIZE;
  }

  public void setBufferPosition(int newPos) {
    this.content.position(PAGE_HEADER_SIZE + newPos);
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o)
      return true;
    if (o == null || getClass() != o.getClass())
      return false;

    final PBasePage pPage = (PBasePage) o;

    if (pageId != null ? !pageId.equals(pPage.pageId) : pPage.pageId != null)
      return false;

    return true;
  }

  @Override
  public int hashCode() {
    return pageId.hashCode();
  }

  @Override
  public String toString() {
    return pageId.toString() + " v=" + version;
  }
}
