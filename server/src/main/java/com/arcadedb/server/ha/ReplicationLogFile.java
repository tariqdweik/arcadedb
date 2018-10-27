/*
 * Copyright (c) 2018 - Arcade Analytics LTD (https://arcadeanalytics.com)
 */

package com.arcadedb.server.ha;

import com.arcadedb.database.Binary;
import com.arcadedb.utility.LockContext;
import com.arcadedb.utility.Pair;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.concurrent.Callable;
import java.util.logging.Level;

/**
 * Replication Log File. Writes the messages to send to a remote node on reconnection.
 */
public class ReplicationLogFile extends LockContext {
  private final HAServer server;
  private final String   filePath;

  private FileChannel channel;

  private static final int        BUFFER_HEADER_SIZE = Binary.LONG_SERIALIZED_SIZE + Binary.INT_SERIALIZED_SIZE;
  private final        ByteBuffer bufferHeader       = ByteBuffer.allocate(BUFFER_HEADER_SIZE);

  private static final int        BUFFER_FOOTER_SIZE = Binary.INT_SERIALIZED_SIZE + Binary.LONG_SERIALIZED_SIZE;
  private final        ByteBuffer bufferFooter       = ByteBuffer.allocate(BUFFER_FOOTER_SIZE);

  private static final long MAGIC_NUMBER = 93719829258702l;

  private long lastMessageNumber = -1;

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

  public ReplicationLogFile(final HAServer server, final String filePath) throws FileNotFoundException {
    this.server = server;
    this.filePath = filePath;
    final File f = new File(filePath);
    if (!f.exists())
      f.getParentFile().mkdirs();

    this.channel = new RandomAccessFile(f, "rw").getChannel();

    final ReplicationMessage lastMessage = getLastMessage();
    if (lastMessage != null)
      lastMessageNumber = lastMessage.messageNumber;
  }

  public void close() {
    executeInLock(new Callable<Object>() {
      @Override
      public Object call() throws Exception {
        channel.close();
        return null;
      }
    });
  }

  public void drop() {
    close();
    new File(filePath).delete();
  }

  public long getLastMessageNumber() {
    return lastMessageNumber;
  }

  public boolean appendMessage(final ReplicationMessage message) {
    return (boolean) executeInLock(new Callable<Object>() {
      @Override
      public Object call() {
        try {
          if (!checkMessageOrder(message))
            return false;

          // UPDATE LAST MESSAGE NUMBER
          lastMessageNumber = message.messageNumber;

          final byte[] content = message.payload.toByteArray();

          final int entrySize = BUFFER_HEADER_SIZE + content.length + BUFFER_FOOTER_SIZE;

          // WRITE HEADER
          bufferHeader.clear();
          bufferHeader.putLong(message.messageNumber);
          bufferHeader.putInt(content.length);
          bufferHeader.rewind();
          channel.write(bufferHeader, channel.size());

          // WRITE PAYLOAD
          channel.write(ByteBuffer.wrap(content), channel.size());

          // WRITE FOOTER
          bufferFooter.clear();
          bufferFooter.putInt(entrySize);
          bufferFooter.putLong(MAGIC_NUMBER);
          bufferFooter.rewind();

          channel.write(bufferFooter, channel.size());

          return true;
        } catch (Exception e) {
          throw new ReplicationLogException("Error on writing message " + message.messageNumber + " to the replication log", e);
        }
      }
    });
  }

  public long findMessagePosition(final long messageNumberToFind) {
    // TODO: CHECK THE LAST MESSAGE AND DECIDE WHERE TO START EITHER FROM THE HEAD OR FROM THE TAIL

    return (long) executeInLock(new Callable<Object>() {
      @Override
      public Object call() throws Exception {
        final long fileSize = channel.size();

        for (long pos = 0; pos < fileSize; ) {
          bufferHeader.clear();
          channel.read(bufferHeader, pos);
          bufferHeader.rewind();

          final long messageNumber = bufferHeader.getLong();
          if (messageNumber == messageNumberToFind)
            // FOUND
            return pos;

          if (messageNumber > messageNumberToFind)
            // NOT IN LOG ANYMORE
            return -1l;

          final int contentLength = bufferHeader.getInt();

          pos += BUFFER_HEADER_SIZE + contentLength + BUFFER_FOOTER_SIZE;
        }

        return -1l;
      }
    });
  }

  public Pair<ReplicationMessage, Long> getMessage(final long pos) {
    return (Pair<ReplicationMessage, Long>) executeInLock(new Callable<Object>() {
      @Override
      public Object call() throws Exception {
        if (pos < 0)
          throw new ReplicationLogException("Invalid position (" + pos + ") in replication log file of size " + channel.size());

        if (pos > channel.size() - BUFFER_HEADER_SIZE - BUFFER_FOOTER_SIZE)
          throw new ReplicationLogException("Invalid position (" + pos + ") in replication log file of size " + channel.size());

        // READ THE HEADER
        bufferHeader.clear();
        channel.read(bufferHeader, pos);
        bufferHeader.rewind();

        final long messageNumber = bufferHeader.getLong();
        final int contentLength = bufferHeader.getInt();

        // READ THE PAYLOAD
        final ByteBuffer bufferPayload = ByteBuffer.allocate(contentLength);
        channel.read(bufferPayload, pos + BUFFER_HEADER_SIZE);

        // READ THE FOOTER
        bufferFooter.clear();
        channel.read(bufferFooter, pos + BUFFER_HEADER_SIZE + contentLength);
        bufferFooter.rewind();

        final int entrySize = bufferFooter.getInt();
        final long magicNumber = bufferFooter.getLong();

        if (magicNumber != MAGIC_NUMBER)
          throw new ReplicationLogException("Corrupted replication log file at position " + pos);

        return new Pair<>(new ReplicationMessage(messageNumber, new Binary((ByteBuffer) bufferPayload.flip())),
            pos + BUFFER_HEADER_SIZE + contentLength + BUFFER_FOOTER_SIZE);
      }
    });
  }

  public boolean checkMessageOrder(final ReplicationMessage message) {
    if (lastMessageNumber > -1) {
      if (message.messageNumber < lastMessageNumber) {
        server.getServer().log(this, Level.WARNING, "Wrong sequence in message numbers. Last was %d and now receiving %d. Skip saving this entry (threadId=%d)",
            lastMessageNumber, message.messageNumber, Thread.currentThread().getId());
        return false;
      }

      if (message.messageNumber != lastMessageNumber + 1) {
        server.getServer().log(this, Level.WARNING, "Found a jump in message numbers. Last was %d and now receiving %d. Skip saving this entry (threadId=%d)",
            lastMessageNumber, message.messageNumber, Thread.currentThread().getId());

        return false;
      }
    }
    return true;
  }

  public ReplicationMessage getLastMessage() {
    return (ReplicationMessage) executeInLock(new Callable<Object>() {
      @Override
      public Object call() throws Exception {
        final long pos = channel.size();
        if (pos == 0)
          // EMPTY FILE
          return null;

        if (pos < BUFFER_HEADER_SIZE + BUFFER_FOOTER_SIZE) {
          // TODO: SCAN FROM THE HEAD
          throw new ReplicationLogException("Invalid position (" + pos + ") in replication log file of size " + channel.size());
        }

        // READ THE FOOTER
        bufferFooter.clear();
        channel.read(bufferFooter, pos - BUFFER_FOOTER_SIZE);
        bufferFooter.rewind();

        final int entrySize = bufferFooter.getInt();
        final long magicNumber = bufferFooter.getLong();

        if (magicNumber != MAGIC_NUMBER)
          throw new ReplicationLogException("Corrupted replication log file");

        // READ THE HEADER
        bufferHeader.clear();
        channel.read(bufferHeader, pos - entrySize);
        bufferHeader.rewind();

        final long messageNumber = bufferHeader.getLong();
        final int contentLength = bufferHeader.getInt();

        // READ THE PAYLOAD
        final ByteBuffer bufferPayload = ByteBuffer.allocate(contentLength);
        channel.read(bufferPayload, pos - entrySize + BUFFER_HEADER_SIZE);

        return new ReplicationMessage(messageNumber, new Binary((ByteBuffer) bufferPayload.flip()));
      }
    });
  }

  public void flush() {
    executeInLock(new Callable<Object>() {
      @Override
      public Object call() throws Exception {
        channel.force(true);
        return null;
      }
    });
  }

  public long getSize() {
    return (long) executeInLock(new Callable<Object>() {
      @Override
      public Object call() throws Exception {
        return channel.size();
      }
    });
  }

  @Override
  public String toString() {
    return filePath;
  }

  @Override
  protected RuntimeException manageExceptionInLock(final Throwable e) {
    if (e instanceof ReplicationLogException)
      throw (ReplicationLogException) e;

    return new ReplicationLogException("Error in replication log", e);
  }

}
