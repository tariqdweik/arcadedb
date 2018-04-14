package com.arcadedb.database;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.concurrent.Callable;

public class PBinary implements PBinaryStructure {
  public static final int BYTE_SERIALIZED_SIZE   = 1;
  public static final int SHORT_SERIALIZED_SIZE  = 2;
  public static final int INT_SERIALIZED_SIZE    = 4;
  public static final int LONG_SERIALIZED_SIZE   = 8;
  public static final int FLOAT_SERIALIZED_SIZE  = 4;
  public static final int DOUBLE_SERIALIZED_SIZE = 8;

  private final static int ALLOCATION_CHUNK = 512;

  protected boolean    autoResizable = true;
  protected byte[]     content;
  protected ByteBuffer buffer;
  protected int        size;

  public PBinary() {
    this.content = new byte[ALLOCATION_CHUNK];
    this.buffer = ByteBuffer.wrap(content);
    size = 0;
  }

  public PBinary(final int initialSize) {
    this.content = new byte[initialSize];
    this.buffer = ByteBuffer.wrap(content);
    size = 0;
  }

  public PBinary(final byte[] buffer, final int contentSize) {
    this.content = buffer;
    this.buffer = ByteBuffer.wrap(content);
    this.size = contentSize;
    this.autoResizable = false;
  }

  public PBinary(final ByteBuffer buffer) {
    this.content = buffer.array();
    this.buffer = buffer;
    this.size = buffer.limit();
    this.autoResizable = false;
  }

  public PBinary copy() {
    final PBinary copy = new PBinary(Arrays.copyOfRange(content, buffer.arrayOffset(), buffer.arrayOffset() + size), size);
    copy.setAutoResizable(autoResizable);
    return copy;
  }

  public void reset() {
    buffer.position(0);
  }

  public boolean isAutoResizable() {
    return autoResizable;
  }

  public void setAutoResizable(final boolean autoResizable) {
    this.autoResizable = autoResizable;
  }

  @Override
  public void append(final PBinary toCopy) {
    final int contentSize = toCopy.size();
    if (contentSize > 0) {
      checkForAllocation(buffer.position(), contentSize);
      buffer.put(toCopy.content, 0, contentSize);
    }
  }

  @Override
  public int position() {
    return buffer.position();
  }

  @Override
  public void position(final int index) {
    buffer.position(index);
  }

  @Override
  public void putByte(final int index, final byte value) {
    checkForAllocation(index, BYTE_SERIALIZED_SIZE);
    buffer.put(index, value);
  }

  @Override
  public void putByte(final byte value) {
    checkForAllocation(buffer.position(), BYTE_SERIALIZED_SIZE);
    buffer.put(value);
  }

  @Override
  public int putNumber(final int index, long value) {
    value = (value << 1) ^ (value >> 63);
    return putUnsignedNumber(index, value);
  }

  @Override
  public int putNumber(long value) {
    value = (value << 1) ^ (value >> 63);
    return putUnsignedNumber(value);
  }

  @Override
  public int putUnsignedNumber(final int index, final long value) {
    position(index);
    return putUnsignedNumber(value);
  }

  @Override
  public int putUnsignedNumber(final long value) {
    int bytesUsed = 0;
    long v = value;
    while ((v & 0xFFFFFFFFFFFFFF80L) != 0L) {
      checkForAllocation(buffer.position(), BYTE_SERIALIZED_SIZE);
      buffer.put((byte) (v & 0x7F | 0x80));
      bytesUsed++;
      v >>>= 7;
    }
    checkForAllocation(buffer.position(), BYTE_SERIALIZED_SIZE);
    buffer.put((byte) (v & 0x7F));
    bytesUsed++;

    return bytesUsed;
  }

  @Override
  public void putShort(final int index, final short value) {
    checkForAllocation(index, SHORT_SERIALIZED_SIZE);
    buffer.putShort(index, value);
  }

  @Override
  public void putShort(final short value) {
    checkForAllocation(buffer.position(), SHORT_SERIALIZED_SIZE);
    buffer.putShort(value);
  }

  @Override
  public void putInt(final int index, final int value) {
    checkForAllocation(index, INT_SERIALIZED_SIZE);
    buffer.putInt(index, value);
  }

  @Override
  public void putInt(final int value) {
    checkForAllocation(buffer.position(), INT_SERIALIZED_SIZE);
    buffer.putInt(value);
  }

  @Override
  public void putLong(final int index, final long value) {
    checkForAllocation(index, LONG_SERIALIZED_SIZE);
    buffer.putLong(index, value);
  }

  @Override
  public void putLong(final long value) {
    checkForAllocation(buffer.position(), LONG_SERIALIZED_SIZE);
    buffer.putLong(value);
  }

  @Override
  public int putString(final int index, final String value) {
    return putBytes(index, value.getBytes());
  }

  @Override
  public int putString(final String value) {
    return putBytes(value.getBytes());
  }

  @Override
  public int putBytes(final int index, final byte[] value) {
    position(index);
    return putBytes(value);
  }

  @Override
  public int putBytes(final byte[] value) {
    int bytesUsed = putNumber(value.length);
    checkForAllocation(buffer.position(), value.length);
    buffer.put(value);
    return bytesUsed + value.length;
  }

  @Override
  public void putByteArray(final int index, final byte[] value) {
    position(index);
    putByteArray(value);
  }

  @Override
  public void putByteArray(final int index, final byte[] value, final int length) {
    position(index);
    putByteArray(value, length);
  }

  @Override
  public void putByteArray(final byte[] value) {
    checkForAllocation(buffer.position(), value.length);
    buffer.put(value);
  }

  @Override
  public void putByteArray(final byte[] value, final int length) {
    checkForAllocation(buffer.position(), length);
    buffer.put(value, 0, length);
  }

  @Override
  public byte getByte(final int index) {
    return buffer.get(index);
  }

  @Override
  public byte getByte() {
    return buffer.get();
  }

  @Override
  public long[] getNumberAndSize(final int index) {
    position(index);
    final long[] raw = getUnsignedNumberAndSize();
    final long temp = (((raw[0] << 63) >> 63) ^ raw[0]) >> 1;
    // This extra step lets us deal with the largest signed values by
    // treating negative results from read unsigned methods as like unsigned values
    // Must re-flip the top bit if the original read value had it set.
    raw[0] = temp ^ (raw[0] & (1L << 63));
    return raw;
  }

  @Override
  public long getNumber(final int index) {
    position(index);
    return getNumber();
  }

  @Override
  public long getNumber() {
    final long raw = getUnsignedNumber();
    final long temp = (((raw << 63) >> 63) ^ raw) >> 1;
    // This extra step lets us deal with the largest signed values by
    // treating negative results from read unsigned methods as like unsigned values
    // Must re-flip the top bit if the original read value had it set.
    return temp ^ (raw & (1L << 63));
  }

  @Override
  public long getUnsignedNumber() {
    long value = 0L;
    int i = 0;
    long b;
    while (((b = buffer.get()) & 0x80L) != 0) {
      value |= (b & 0x7F) << i;
      i += 7;
      if (i > 63)
        throw new IllegalArgumentException("Variable length quantity is too long (must be <= 63)");
    }
    return value | (b << i);
  }

  @Override
  public long[] getUnsignedNumberAndSize() {
    long value = 0L;
    int i = 0;
    long b;
    int byteRead = 1;
    while (((b = buffer.get()) & 0x80L) != 0) {
      value |= (b & 0x7F) << i;
      i += 7;
      if (i > 63)
        throw new IllegalArgumentException("Variable length quantity is too long (must be <= 63)");
      ++byteRead;
    }
    return new long[] { value | (b << i), byteRead };
  }

  @Override
  public short getShort(final int index) {
    return buffer.getShort(index);
  }

  @Override
  public short getShort() {
    return buffer.getShort();
  }

  @Override
  public short getUnsignedShort() {
    int firstByte = (0x000000FF & ((int) buffer.get()));
    int secondByte = (0x000000FF & ((int) buffer.get()));
    return (short) (firstByte << 8 | secondByte);
  }

  @Override
  public int getInt() {
    return buffer.getInt();
  }

  @Override
  public int getInt(final int index) {
    return buffer.getInt(index);
  }

  @Override
  public long getLong() {
    return buffer.getLong();
  }

  @Override
  public long getLong(final int index) {
    return buffer.getLong(index);
  }

  @Override
  public String getString() {
    return new String(getBytes());
  }

  @Override
  public String getString(final int index) {
    return new String(getBytes(index));
  }

  @Override
  public void getByteArray(final byte[] buffer) {
    this.buffer.get(buffer);
  }

  @Override
  public void getByteArray(final int index, final byte[] buffer) {
    this.buffer.position(index);
    this.buffer.get(buffer);
  }

  @Override
  public void getByteArray(final int index, final byte[] buffer, final int offset, final int length) {
    this.buffer.position(index);
    this.buffer.get(buffer, offset, length);
  }

  @Override
  public byte[] getBytes() {
    final byte[] result = new byte[(int) getNumber()];
    buffer.get(result);
    return result;
  }

  @Override
  public byte[] getBytes(final int index) {
    buffer.position(index);
    return getBytes();
  }

  @Override
  public byte[] toByteArray() {
    final byte[] result = new byte[size];
    System.arraycopy(content, buffer.arrayOffset(), result, 0, result.length);
    return result;
  }

  @Override
  public ByteBuffer getByteBuffer() {
    return buffer;
  }

  public void flip() {
    size = buffer.position();
    buffer.flip();
  }

  /**
   * Creates a copy of this object referring to the same underlying buffer.
   */
  public PBinary slice() {
    buffer.position(0);
    return new PBinary(buffer.slice());
  }

  /**
   * Creates a copy of this object referring to the same underlying buffer, starting from a position.
   */
  public PBinary slice(final int position) {
    buffer.position(position);
    return new PBinary(buffer.slice());
  }

  /**
   * Creates a copy of this object referring to the same underlying buffer, starting from a position and with a custom length.
   */
  public PBinary slice(final int position, final int length) {
    buffer.position(position);
    final ByteBuffer result = buffer.slice();
    result.position(length);
    result.flip();
    return new PBinary(result);
  }

  @Override
  public int size() {
    return size;
  }

  public void size(int newSize) {
    if (newSize > content.length)
      checkForAllocation(0, newSize);
    else
      size = newSize;
  }

  public void move(final int startPosition, final int destPosition, final int length) {
    System.arraycopy(content, buffer.arrayOffset() + startPosition, content, buffer.arrayOffset() + destPosition, length);
  }

  public byte[] getContent() {
    return content;
  }

  public int getContentSize() {
    return content.length;
  }

  @Override
  public String toString() {
    return "PBinary size=" + size + " pos=" + buffer.position();
  }

  /**
   * Allocates enough space (max 1 page) and update the size according to the bytes to write.
   */
  protected void checkForAllocation(final int offset, final int bytesToWrite) {
    if (offset + bytesToWrite > content.length) {

      if (!autoResizable)
        throw new IllegalArgumentException("Cannot resize the buffer (autoResizable=false)");

      final int newSize;
      if (offset + bytesToWrite > ALLOCATION_CHUNK) {
        newSize = ALLOCATION_CHUNK * ((offset + bytesToWrite / ALLOCATION_CHUNK) + 1);
      } else
        newSize = ALLOCATION_CHUNK;

      final byte[] newContent = new byte[newSize];
      System.arraycopy(content, 0, newContent, 0, content.length);
      this.content = newContent;

      final int oldPosition = this.buffer.position();
      final int oldOffset = this.buffer.arrayOffset();
      this.buffer = ByteBuffer.wrap(this.content, oldOffset, this.content.length);
      this.buffer.position(oldPosition);
    }

    if (offset + bytesToWrite > size)
      size = offset + bytesToWrite;
  }

  public Object executeInLock(final Callable<Object> callable) throws Exception {
    return callable.call();
  }
}
