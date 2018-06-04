/*
 * Copyright (c) 2018 - Arcade Analytics LTD (https://arcadeanalytics.com)
 */
package com.arcadedb.network.binary;

import com.arcadedb.ContextConfiguration;
import com.arcadedb.GlobalConfiguration;

import java.io.*;
import java.net.*;

public class ChannelBinaryClient extends ChannelBinary {
  protected final int    socketTimeout;
  protected       String url;

  public ChannelBinaryClient(final String remoteHost, final int remotePort, final ContextConfiguration config) throws IOException {
    super(SocketFactory.instance(config).createSocket());
    try {

      url = remoteHost + ":" + remotePort;
      socketTimeout = config.getValueAsInteger(GlobalConfiguration.NETWORK_SOCKET_TIMEOUT);

      try {
        if (remoteHost.contains(":")) {
          // IPV6
          final InetAddress[] addresses = Inet6Address.getAllByName(remoteHost);
          socket.connect(new InetSocketAddress(addresses[0], remotePort), socketTimeout);

        } else {
          // IPV4
          socket.connect(new InetSocketAddress(remoteHost, remotePort), socketTimeout);
        }
        setReadResponseTimeout();

      } catch (SocketTimeoutException e) {
        throw new IOException("Cannot connect to host " + remoteHost + ":" + remotePort + " (timeout=" + socketTimeout + ")", e);
      }
      try {
        if (socketBufferSize > 0) {
          inStream = new BufferedInputStream(socket.getInputStream(), socketBufferSize);
          outStream = new BufferedOutputStream(socket.getOutputStream(), socketBufferSize);
        } else {
          inStream = new BufferedInputStream(socket.getInputStream());
          outStream = new BufferedOutputStream(socket.getOutputStream());
        }

        in = new DataInputStream(inStream);
        out = new DataOutputStream(outStream);

      } catch (IOException e) {
        throw new NetworkProtocolException("Error on reading data from remote server " + socket.getRemoteSocketAddress() + ": ", e);
      }

    } catch (RuntimeException e) {
      if (socket.isConnected())
        socket.close();
      throw e;
    }
  }

  /**
   * Tells if the channel is connected.
   *
   * @return true if it's connected, otherwise false.
   */
  public boolean isConnected() {
    final Socket s = socket;
    return s != null && !s.isClosed() && s.isConnected() && !s.isInputShutdown() && !s.isOutputShutdown();
  }

  protected void setReadResponseTimeout() throws SocketException {
    final Socket s = socket;
    if (s != null && s.isConnected() && !s.isClosed())
      s.setSoTimeout(socketTimeout);
  }

  public String getURL() {
    return url;
  }
}
