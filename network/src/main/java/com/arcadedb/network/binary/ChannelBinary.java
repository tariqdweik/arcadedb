/*
 * Copyright (c) 2018 - Arcade Analytics LTD (https://arcadeanalytics.com)
 */
package com.arcadedb.network.binary;

import com.arcadedb.database.Binary;
import com.arcadedb.database.Database;
import com.arcadedb.database.RID;
import com.arcadedb.utility.LogManager;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.Socket;
import java.util.Arrays;

/**
 * Abstract representation of a channel.
 **/
public abstract class ChannelBinary extends Channel implements ChannelDataInput, ChannelDataOutput {
  private static final int              MAX_LENGTH_DEBUG = 150;
  protected final      boolean          debug;
  private final        int              maxChunkSize;
  protected            DataInputStream  in;
  protected            DataOutputStream out;

  public ChannelBinary(final Socket iSocket) throws IOException {
    super(iSocket);

    maxChunkSize = 16384 * 1024;
    debug = false;

    if (debug)
      LogManager.instance().info(this, "%s - Connected", socket.getRemoteSocketAddress());
  }

  public byte readByte() throws IOException {
    updateMetricReceivedBytes(Binary.BYTE_SERIALIZED_SIZE);

    if (debug) {
      LogManager.instance().info(this, "%s - Reading byte (1 byte)...", socket.getRemoteSocketAddress());
      final byte value = in.readByte();
      LogManager.instance().info(this, "%s - Read byte: %d", socket.getRemoteSocketAddress(), (int) value);
      return value;
    }

    return in.readByte();
  }

  public boolean readBoolean() throws IOException {
    updateMetricReceivedBytes(Binary.BYTE_SERIALIZED_SIZE);

    if (debug) {
      LogManager.instance().info(this, "%s - Reading boolean (1 byte)...", socket.getRemoteSocketAddress());
      final boolean value = in.readBoolean();
      LogManager.instance().info(this, "%s - Read boolean: %b", socket.getRemoteSocketAddress(), value);
      return value;
    }

    return in.readBoolean();
  }

  public int readInt() throws IOException {
    updateMetricReceivedBytes(Binary.INT_SERIALIZED_SIZE);

    if (debug) {
      LogManager.instance().info(this, "%s - Reading int (4 bytes)...", socket.getRemoteSocketAddress());
      final int value = in.readInt();
      LogManager.instance().info(this, "%s - Read int: %d", socket.getRemoteSocketAddress(), value);
      return value;
    }

    return in.readInt();
  }

  public long readLong() throws IOException {
    updateMetricReceivedBytes(Binary.LONG_SERIALIZED_SIZE);

    if (debug) {
      LogManager.instance().info(this, "%s - Reading long (8 bytes)...", socket.getRemoteSocketAddress());
      final long value = in.readLong();
      LogManager.instance().info(this, "%s - Read long: %d", socket.getRemoteSocketAddress(), value);
      return value;
    }

    return in.readLong();
  }

  public short readShort() throws IOException {
    updateMetricReceivedBytes(Binary.SHORT_SERIALIZED_SIZE);

    if (debug) {
      LogManager.instance().info(this, "%s - Reading short (2 bytes)...", socket.getRemoteSocketAddress());
      final short value = in.readShort();
      LogManager.instance().info(this, "%s - Read short: %d", socket.getRemoteSocketAddress(), value);
      return value;
    }

    return in.readShort();
  }

  public String readString() throws IOException {
    if (debug) {
      LogManager.instance().info(this, "%s - Reading string (4+N bytes)...", socket.getRemoteSocketAddress());
      final int len = in.readInt();
      if (len < 0)
        return null;

      // REUSE STATIC BUFFER?
      final byte[] tmp = new byte[len];
      in.readFully(tmp);

      updateMetricReceivedBytes(Binary.INT_SERIALIZED_SIZE + len);

      final String value = new String(tmp, "UTF-8");
      LogManager.instance().info(this, "%s - Read string: %s", socket.getRemoteSocketAddress(), value);
      return value;
    }

    final int len = in.readInt();
    if (len < 0)
      return null;

    final byte[] tmp = new byte[len];
    in.readFully(tmp);

    updateMetricReceivedBytes(Binary.INT_SERIALIZED_SIZE + len);

    return new String(tmp, "UTF-8");
  }

  public byte[] readBytes() throws IOException {
    if (debug)
      LogManager.instance().info(this, "%s - Reading chunk of bytes. Reading chunk length as int (4 bytes)...", socket.getRemoteSocketAddress());

    final int len = in.readInt();
    if (len > maxChunkSize) {
      throw new IOException(
          "Impossible to read a chunk of length:" + len + " max allowed chunk length:" + maxChunkSize + " see NETWORK_BINARY_MAX_CONTENT_LENGTH settings ");
    }
    updateMetricReceivedBytes(Binary.INT_SERIALIZED_SIZE + len);

    if (debug)
      LogManager.instance().info(this, "%s - Read chunk length: %d", socket.getRemoteSocketAddress(), len);

    if (len < 0)
      return null;

    if (debug)
      LogManager.instance().info(this, "%s - Reading %d bytes...", socket.getRemoteSocketAddress(), len);

    // REUSE STATIC BUFFER?
    final byte[] tmp = new byte[len];
    in.readFully(tmp);

    if (debug)
      LogManager.instance().info(this, "%s - Read %d bytes: %s", socket.getRemoteSocketAddress(), len, new String(tmp));

    return tmp;
  }

  public RID readRID(final Database database) throws IOException {
    final int clusterId = readInt();
    final long clusterPosition = readLong();
    return new RID(database, clusterId, clusterPosition);
  }

  public int readVersion() throws IOException {
    return readInt();
  }

  public ChannelBinary writeByte(final byte iContent) throws IOException {
    if (debug)
      LogManager.instance().info(this, "%s - Writing byte (1 byte): %d", socket.getRemoteSocketAddress(), iContent);

    out.write(iContent);
    updateMetricTransmittedBytes(Binary.BYTE_SERIALIZED_SIZE);
    return this;
  }

  public ChannelBinary writeBoolean(final boolean iContent) throws IOException {
    if (debug)
      LogManager.instance().info(this, "%s - Writing boolean (1 byte): %b", socket.getRemoteSocketAddress(), iContent);

    out.writeBoolean(iContent);
    updateMetricTransmittedBytes(Binary.BYTE_SERIALIZED_SIZE);
    return this;
  }

  public ChannelBinary writeInt(final int iContent) throws IOException {
    if (debug)
      LogManager.instance().info(this, "%s - Writing int (4 bytes): %d", socket.getRemoteSocketAddress(), iContent);

    out.writeInt(iContent);
    updateMetricTransmittedBytes(Binary.INT_SERIALIZED_SIZE);
    return this;
  }

  public ChannelBinary writeLong(final long iContent) throws IOException {
    if (debug)
      LogManager.instance().info(this, "%s - Writing long (8 bytes): %d", socket.getRemoteSocketAddress(), iContent);

    out.writeLong(iContent);
    updateMetricTransmittedBytes(Binary.LONG_SERIALIZED_SIZE);
    return this;
  }

  public ChannelBinary writeShort(final short iContent) throws IOException {
    if (debug)
      LogManager.instance().info(this, "%s - Writing short (2 bytes): %d", socket.getRemoteSocketAddress(), iContent);

    out.writeShort(iContent);
    updateMetricTransmittedBytes(Binary.SHORT_SERIALIZED_SIZE);
    return this;
  }

  public ChannelBinary writeString(final String iContent) throws IOException {
    if (debug)
      LogManager.instance().info(this, "%s - Writing string (4+%d=%d bytes): %s", socket.getRemoteSocketAddress(), iContent != null ? iContent.length() : 0,
          iContent != null ? iContent.length() + 4 : 4, iContent);

    if (iContent == null) {
      out.writeInt(-1);
      updateMetricTransmittedBytes(Binary.INT_SERIALIZED_SIZE);
    } else {
      final byte[] buffer = iContent.getBytes("UTF-8");
      out.writeInt(buffer.length);
      out.write(buffer, 0, buffer.length);
      updateMetricTransmittedBytes(Binary.INT_SERIALIZED_SIZE + buffer.length);
    }

    return this;
  }

  public ChannelBinary writeBytes(final byte[] iContent) throws IOException {
    return writeBytes(iContent, iContent != null ? iContent.length : 0);
  }

  public ChannelBinary writeBytes(final byte[] iContent, final int iLength) throws IOException {
    if (debug)
      LogManager.instance()
          .info(this, "%s - Writing bytes (4+%d=%d bytes): %s", socket.getRemoteSocketAddress(), iLength, iLength + 4, Arrays.toString(iContent));

    if (iContent == null) {
      out.writeInt(-1);
      updateMetricTransmittedBytes(Binary.INT_SERIALIZED_SIZE);
    } else {
      if (iLength > maxChunkSize) {
        throw new IOException("Impossible to write a chunk of length:" + iLength + " max allowed chunk length:" + maxChunkSize
            + " see NETWORK_BINARY_MAX_CONTENT_LENGTH settings ");
      }

      out.writeInt(iLength);
      out.write(iContent, 0, iLength);
      updateMetricTransmittedBytes(Binary.INT_SERIALIZED_SIZE + iLength);
    }
    return this;
  }

  public void writeRID(final RID iRID) throws IOException {
    writeInt(iRID.getBucketId());
    writeLong(iRID.getPosition());
  }

  public void writeVersion(final int version) throws IOException {
    writeInt(version);
  }

  public void clearInput() throws IOException {
    if (in == null)
      return;

    final StringBuilder dirtyBuffer = new StringBuilder(MAX_LENGTH_DEBUG);
    int i = 0;
    while (in.available() > 0) {
      char c = (char) in.read();
      ++i;

      if (dirtyBuffer.length() < MAX_LENGTH_DEBUG)
        dirtyBuffer.append(c);
    }
    updateMetricReceivedBytes(i);

    final String message = "Received unread response from " + socket.getRemoteSocketAddress()
        + " probably corrupted data from the network connection. Cleared dirty data in the buffer (" + i + " bytes): [" + dirtyBuffer + (
        i > dirtyBuffer.length() ? "..." : "") + "]";
    LogManager.instance().error(this, message, null);
    throw new IOException(message);

  }

  @Override
  public void flush() throws IOException {
    if (debug)
      LogManager.instance().info(this, "%s - Flush", socket != null ? " null possible previous close" : socket.getRemoteSocketAddress());

    updateMetricFlushes();

    if (out != null)
      // IT ALREADY CALL THE UNDERLYING FLUSH
      out.flush();
    else
      super.flush();
  }

  @Override
  public void close() {
    if (debug)
      LogManager.instance().info(this, "%s - Closing socket...", socket != null ? " null possible previous close" : socket.getRemoteSocketAddress());

    try {
      if (in != null) {
        in.close();
      }
    } catch (IOException e) {
      LogManager.instance().debug(this, "Error during closing of input stream", e);
    }

    try {
      if (out != null) {
        out.close();
      }
    } catch (IOException e) {
      LogManager.instance().debug(this, "Error during closing of output stream", e);
    }

    super.close();
  }

  public DataOutputStream getDataOutput() {
    return out;
  }

  public DataInputStream getDataInput() {
    return in;
  }
}
