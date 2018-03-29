package com.arcadedb.engine;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

public class PWALFile {
  private final String      filePath;
  private       FileChannel channel;
  private       boolean     open;

  public PWALFile(final String filePath) throws FileNotFoundException {
    this.filePath = filePath;
    this.channel = new RandomAccessFile(filePath, "rw").getChannel();
    this.open = true;
  }

  public void close() throws IOException {
    channel.close();
    this.open = false;
  }

  public void drop() throws IOException {
    close();
    new File(getFilePath()).delete();
  }

  public void flush() throws IOException {
    channel.force(false);
  }

  public long getSize() throws IOException {
    return channel.size();
  }

  /**
   * Returns the byte written.
   */
  public synchronized void append(final ByteBuffer buffer, final boolean sync) throws IOException {
    buffer.rewind();
    channel.write(buffer, channel.size());
    if (sync)
      channel.force(true);
  }

  public boolean isOpen() {
    return open;
  }

  public String getFilePath() {
    return filePath;
  }

  @Override
  public String toString() {
    return filePath;
  }
}
