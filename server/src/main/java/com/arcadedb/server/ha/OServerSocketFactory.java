/*
 * Copyright (c) 2018 - Arcade Analytics LTD (https://arcadeanalytics.com)
 */
package com.arcadedb.server.ha;

import java.io.IOException;
import java.net.InetAddress;
import java.net.ServerSocket;

public abstract class OServerSocketFactory {

  private static OServerSocketFactory theFactory;

  public OServerSocketFactory() {
  }

  public static OServerSocketFactory getDefault() {
    synchronized (OServerSocketFactory.class) {
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

