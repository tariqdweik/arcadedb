package com.arcadedb.engine;

import com.arcadedb.database.PBinary;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.concurrent.Callable;

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

  protected final    PPageId pageId;
  protected final    PBinary content;
  private final      int     size;
  protected volatile int     version;
  private            long    lastAccessed = System.currentTimeMillis();

  protected PBasePage(final PPageManager manager, final PPageId pageId, final int size, final byte[] buffer, final int version,
      final int contentSize) {
    this.manager = manager;
    this.pageId = pageId;
    this.size = size;
    this.content = new PBinary(buffer, contentSize);
    this.version = version;
  }

  public PModifiablePage modify() {
    final byte[] array = this.content.getByteBuffer().array();
    final PModifiablePage copy = new PModifiablePage(manager, pageId, size, Arrays.copyOf(array, array.length), version,
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

  /**
   * Creates an immutable copy. The content is not copied (the same byte[] is used), because after invoking this method the original page is never modified.
   */
  public PImmutablePage createImmutableCopy() {
    try {
      return (PImmutablePage) content.executeInLock(new Callable<Object>() {
        @Override
        public PImmutablePage call() throws Exception {
          return new PImmutablePage(manager, pageId, getPhysicalSize(), content.getByteBuffer().array(), version, content.size());
        }
      });
    } catch (RuntimeException e) {
      throw e;
    } catch (Exception e) {
      throw new RuntimeException("Cannot create an immutable copy of page " + toString(), e);
    }
  }

  public int getAvailableContentSize() {
    return getPhysicalSize() - getContentSize();
  }

  public int getContentSize() {
    return content.size() - PAGE_HEADER_SIZE;
  }

  public void setContentSize(final int value) {
    content.size(value + PAGE_HEADER_SIZE);
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

  public void readByteArray(final int index, final byte[] buffer, final int offset, final int length) {
    this.content.getByteArray(PAGE_HEADER_SIZE + index, buffer, offset, length);
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
    return content.slice(index + PAGE_HEADER_SIZE, length);
  }

  public PPageId getPageId() {
    return pageId;
  }

  /**
   * Returns the underlying ByteBuffer. If any changes occur bypassing the page object, must be tracked by calling #updateModifiedRange() method.
   */
  public ByteBuffer getContent() {
    return content.getByteBuffer();
  }

  /**
   * Returns the underlying ByteBuffer. If any changes occur bypassing the page object, must be tracked by calling #updateModifiedRange() method.
   */
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

    final PBasePage other = (PBasePage) o;

    if (pageId != null ? !pageId.equals(other.pageId) : other.pageId != null)
      return false;

    return version == other.version;
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
