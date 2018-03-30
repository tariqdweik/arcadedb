package com.arcadedb.engine;

import com.arcadedb.utility.PLockContext;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.concurrent.atomic.AtomicInteger;

public class PWALFile extends PLockContext {
  private final String      filePath;
  private       FileChannel channel;
  private       boolean     open;
  private AtomicInteger pagesToFlush = new AtomicInteger();

  public PWALFile(final String filePath) throws FileNotFoundException {
    super(true);
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

  public int getPagesToFlush() {
    return pagesToFlush.get();
  }

  public void notifyPageFlushed() {
    pagesToFlush.decrementAndGet();
  }

  public long getSize() throws IOException {
    return channel.size();
  }

  public void appendPage(final ByteBuffer buffer) throws IOException {
    pagesToFlush.incrementAndGet();
    append(buffer);
  }

  public void append(final ByteBuffer buffer) throws IOException {
    buffer.rewind();
    channel.write(buffer, channel.size());
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
