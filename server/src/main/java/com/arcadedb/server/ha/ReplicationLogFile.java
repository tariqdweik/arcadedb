/*
 * Copyright (c) 2018 - Arcade Analytics LTD (https://arcadeanalytics.com)
 */

package com.arcadedb.server.ha;

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.database.Binary;
import com.arcadedb.utility.LogManager;

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
  private static final int        BUFFER_HEADER_SIZE = Binary.LONG_SERIALIZED_SIZE + Binary.INT_SERIALIZED_SIZE;
  private final        ByteBuffer bufferHeader       = ByteBuffer.allocate(BUFFER_HEADER_SIZE);
  private              long       maxSize            = GlobalConfiguration.HA_REPLICATION_FILE_MAXSIZE.getValueAsLong();

  private long firstMessageNumber = -1;
  private long lastMessageNumber  = -1;

  public static class Entry {
    public final long   messageNumber;
    public final Binary payload;
    public final int    length;

    public Entry(final long messageNumber, final Binary payload, final int length) {
      this.messageNumber = messageNumber;
      this.payload = payload;
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

  public boolean append(final long messageNumber, final Binary buffer) throws IOException {
    if (firstMessageNumber == -1)
      firstMessageNumber = messageNumber;
    if (lastMessageNumber > -1 && messageNumber != lastMessageNumber + 1)
      LogManager.instance()
          .info(this, "Found a jump in message numbers. Last was %d and now receiving %d", lastMessageNumber, messageNumber);

    lastMessageNumber = messageNumber;

    final byte[] content = buffer.toByteArray();

    if (content.length + Binary.INT_SERIALIZED_SIZE > maxSize)
      return false;

    bufferHeader.clear();
    bufferHeader.putLong(messageNumber);
    bufferHeader.putInt(content.length);
    bufferHeader.flip();
    channel.write(bufferHeader, channel.size());

    channel.write(ByteBuffer.wrap(content), channel.size());

    return true;
  }

  public Entry getEntry(final long pos) throws IOException {
    bufferHeader.clear();
    channel.read(bufferHeader, pos);
    final long messageNumber = bufferHeader.getLong(0);
    final int length = bufferHeader.getInt(Binary.LONG_SERIALIZED_SIZE);

    final ByteBuffer buffer = ByteBuffer.allocate(length);
    channel.read(buffer, pos + BUFFER_HEADER_SIZE);

    return new Entry(messageNumber, new Binary((ByteBuffer) buffer.flip()), BUFFER_HEADER_SIZE + length);
  }

  public long getFirstMessageNumber() {
    return firstMessageNumber;
  }

  public long getLastMessageNumber() {
    return lastMessageNumber;
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
