/*
 * Copyright (c) 2018 - Arcade Analytics LTD (https://arcadeanalytics.com)
 */

package com.arcadedb.server.ha;

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.database.Binary;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

/**
 * Replication Log File. Writes the messages to send to a remote node on reconnection.
 */
public class ReplicationLogFile {
  private final String      filePath;
  private       FileChannel channel;

  // STATIC BUFFERS USED FOR RECOVERY
  private final ByteBuffer bufferInt = ByteBuffer.allocate(Binary.INT_SERIALIZED_SIZE);
  private       long       maxSize   = GlobalConfiguration.HA_REPLICATION_FILE_MAXSIZE.getValueAsLong();

  public static class Entry {
    public Binary message;
    public int    length;

    public Entry(final Binary message, final int length) {
      this.message = message;
      this.length = length;
    }
  }

  public ReplicationLogFile(final String filePath) throws FileNotFoundException {
    this.filePath = filePath;
    final File f = new File(filePath);
    if (f.exists())
      f.delete();
    else
      f.getParentFile().mkdirs();

    this.channel = new RandomAccessFile(f, "rw").getChannel();
  }

  public void close() throws IOException {
    channel.close();
  }

  public void drop() throws IOException {
    close();
    new File(getFilePath()).delete();
  }

  public boolean append(final Binary buffer) throws IOException {
    final byte[] content = buffer.toByteArray();

    if (content.length + Binary.INT_SERIALIZED_SIZE > maxSize)
      return false;

    bufferInt.clear();
    bufferInt.putInt(content.length);
    bufferInt.flip();
    channel.write(bufferInt, channel.size());

    channel.write(ByteBuffer.wrap(content), channel.size());

    return true;
  }

  public Entry getEntry(long pos) throws IOException {
    bufferInt.clear();
    channel.read(bufferInt, pos);
    final int length = bufferInt.getInt(0);

    final ByteBuffer buffer = ByteBuffer.allocate(length);
    channel.read(buffer, pos + Binary.INT_SERIALIZED_SIZE);

    return new Entry(new Binary((ByteBuffer) buffer.flip()), Binary.INT_SERIALIZED_SIZE + length);
  }

  public void flush() throws IOException {
    channel.force(true);
  }

  public long getSize() throws IOException {
    return channel.size();
  }

  public String getFilePath() {
    return filePath;
  }

  @Override
  public String toString() {
    return filePath;
  }
}
