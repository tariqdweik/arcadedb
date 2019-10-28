/*
 * Copyright (c) - Arcade Analytics LTD (https://arcadeanalytics.com)
 */
package com.arcadedb.redisw;

import com.arcadedb.Constants;
import com.arcadedb.network.binary.ChannelBinaryServer;
import com.arcadedb.server.ArcadeDBServer;

import java.io.EOFException;
import java.io.IOException;
import java.net.Socket;
import java.net.SocketException;
import java.net.SocketTimeoutException;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Level;

public class RedisNetworkExecutor extends Thread {
  private static final byte[]              LF       = new byte[] { '\r', '\n' };
  private final        ArcadeDBServer      server;
  private              ChannelBinaryServer channel;
  private volatile     boolean             shutdown = false;

  private       int           posInBuffer = 0;
  private final StringBuilder value       = new StringBuilder();
  private final byte[]        buffer      = new byte[32 * 1024];
  private       int           bytesRead   = 0;

  public RedisNetworkExecutor(final ArcadeDBServer server, final Socket socket) throws IOException {
    setName(Constants.PRODUCT + "-redis/" + socket.getInetAddress());
    this.server = server;
    this.channel = new ChannelBinaryServer(socket, server.getConfiguration());
  }

  @Override
  public void run() {
    while (!shutdown) {
      try {
        executeCommand(parseNext());

        replyToClient(value);

      } catch (EOFException | SocketException e) {
        server.log(this, Level.FINE, "Redis wrapper: Error on reading request", e);
        close();
      } catch (SocketTimeoutException e) {
        // IGNORE IT
      } catch (IOException e) {
        server.log(this, Level.SEVERE, "Redis wrapper: Error on reading request", e);
      }
    }
  }

  private void executeCommand(final Object command) {
    value.setLength(0);

    if (command instanceof List) {
      final List<Object> list = (List<Object>) command;
      if (list.isEmpty())
        return;

      final Object cmd = list.get(0);
      if (!(cmd instanceof String))
        server.log(this, Level.SEVERE, "Redis wrapper: Invalid command[0] %s (type=%s)", command, cmd.getClass());

      final String cmdString = (String) cmd;

      if (cmdString.equals("GET")) {
        value.append("+bar\r\n");
      } else if (cmdString.equals("SET")) {
        value.append("+OK\r\n");
      }

    } else
      server.log(this, Level.SEVERE, "Redis wrapper: Invalid command %s", command);
  }

  private Object parseNext() throws IOException {
    final byte b = readNext();

    if (b == '+')
      // SIMPLE STRING
      return parseValueUntilLF();
    else if (b == ':')
      // INTEGER
      return Integer.parseInt(parseValueUntilLF());
    else if (b == '$') {
      // BATCH STRING
      final String value = parseChars(Integer.parseInt(parseValueUntilLF()));
      skipLF();
      return value;
    } else if (b == '*') {
      // ARRAY
      final List<Object> array = new ArrayList<>();
      final int arraySize = Integer.parseInt(parseValueUntilLF());
      for (int i = 0; i < arraySize; ++i)
        array.add(parseNext());
      return array;
    } else {
      server.log(this, Level.SEVERE, "Redis wrapper: Invalid character '%s'", (char) b);
      return null;
    }
  }

  private void skipLF() throws IOException {
    final byte b = readNext();
    if (b == '\r') {
      final byte b2 = readNext();
      if (b2 == '\n') {
      } else
        server.log(this, Level.SEVERE, "Redis wrapper: Invalid character '%s' instead of expected \\n", (char) b2);
    } else
      server.log(this, Level.SEVERE, "Redis wrapper: Invalid character '%s' instead of expected \\r", (char) b);
  }

  private String parseValueUntilLF() throws IOException {
    value.setLength(0);

    boolean slashR = false;

    while (!shutdown) {
      final byte b = readNext();

      if (!slashR) {
        if (b == '\r')
          slashR = true;
        else
          value.append((char) b);
      } else {
        if (b == '\n')
          break;
        else
          server.log(this, Level.SEVERE, "Redis wrapper: Error on parsing value waiting for LF, but found '%s' after /r", (char) b);
      }
    }

    return value.toString();
  }

  private String parseChars(final int size) throws IOException {
    value.setLength(0);

    for (int i = 0; i < size && !shutdown; ++i) {
      final byte b = readNext();
      value.append((char) b);
    }

    return value.toString();
  }

  private byte readNext() throws IOException {
    if (posInBuffer < bytesRead)
      return buffer[posInBuffer++];

    posInBuffer = 0;

    do {
      bytesRead = channel.inStream.read(buffer);

//      String debug = "";
//      for (int i = 0; i < bytesRead; ++i) {
//        debug += (char) buffer[i];
//      }
//      server.log(this, Level.INFO, "Redis wrapper: Read '%s'...", debug);

    } while (bytesRead == 0);

    if (bytesRead == -1)
      throw new EOFException();

    return buffer[posInBuffer++];
  }

  public void replyToClient(final StringBuilder response) throws IOException {
    server.log(this, Level.FINE, "Redis wrapper: Sending response back to the client '%s'...", response);

    final byte[] buffer = response.toString().getBytes();

    channel.outStream.write(buffer);
    channel.flush();

    response.setLength(0);
  }

  public void close() {
    shutdown = true;
    if (channel != null)
      channel.close();
  }

  public String getURL() {
    return channel.getLocalSocketAddress();
  }

  public byte[] receiveResponse() throws IOException {
    return channel.readBytes();
  }
}
