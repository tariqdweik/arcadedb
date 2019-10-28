/*
 * Copyright (c) - Arcade Analytics LTD (https://arcadeanalytics.com)
 */
package com.arcadedb.server.ha.network;

import java.io.IOException;
import java.net.InetAddress;
import java.net.ServerSocket;

public abstract class ServerSocketFactory {

  private static ServerSocketFactory theFactory;

  public ServerSocketFactory() {
  }

  public static ServerSocketFactory getDefault() {
    synchronized (ServerSocketFactory.class) {
      if (theFactory == null) {
        theFactory = new DefaultServerSocketFactory();
      }
    }

    return theFactory;
  }

  public abstract ServerSocket createServerSocket(int port) throws IOException;

  public abstract ServerSocket createServerSocket(int port, int backlog) throws IOException;

  public abstract ServerSocket createServerSocket(int port, int backlog, InetAddress ifAddress) throws IOException;
}

