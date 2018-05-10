/*
 * Copyright (c) 2018 - Arcade Analytics LTD (https://arcadeanalytics.com)
 */
package com.arcadedb.server.ha;

import com.arcadedb.Constants;
import com.arcadedb.ContextConfiguration;
import com.arcadedb.network.binary.Channel;
import com.arcadedb.server.ArcadeDBServer;
import com.arcadedb.server.ServerException;
import com.arcadedb.utility.LogManager;

import java.io.IOException;
import java.net.*;

public class ServerNetworkListener extends Thread {
  private          OServerSocketFactory socketFactory;
  private          ServerSocket         serverSocket;
  private          InetSocketAddress    inboundAddr;
  private volatile boolean              active          = true;
  private          int                  socketBufferSize;
  private          ContextConfiguration configuration;
  private          ArcadeDBServer       server;
  private          int                  protocolVersion = -1;

  public ServerNetworkListener(final ArcadeDBServer iServer, final OServerSocketFactory iSocketFactory, final String iHostName,
      final String iHostPortRange) {
    super(Constants.PRODUCT + " replication listen at " + iHostName + ":" + iHostPortRange);
    server = iServer;

    socketFactory = iSocketFactory == null ? OServerSocketFactory.getDefault() : iSocketFactory;

    listen(iHostName, iHostPortRange);

    start();
  }

  public static int[] getPorts(final String iHostPortRange) {
    int[] ports;

    if (iHostPortRange.contains(",")) {
      // MULTIPLE ENUMERATED PORTS
      String[] portValues = iHostPortRange.split(",");
      ports = new int[portValues.length];
      for (int i = 0; i < portValues.length; ++i)
        ports[i] = Integer.parseInt(portValues[i]);

    } else if (iHostPortRange.contains("-")) {
      // MULTIPLE RANGE PORTS
      String[] limits = iHostPortRange.split("-");
      int lowerLimit = Integer.parseInt(limits[0]);
      int upperLimit = Integer.parseInt(limits[1]);
      ports = new int[upperLimit - lowerLimit + 1];
      for (int i = 0; i < upperLimit - lowerLimit + 1; ++i)
        ports[i] = lowerLimit + i;

    } else
      // SINGLE PORT SPECIFIED
      ports = new int[] { Integer.parseInt(iHostPortRange) };

    return ports;
  }

  public void close() {
    this.active = false;

    if (serverSocket != null)
      try {
        serverSocket.close();
      } catch (IOException e) {
      }
  }

  public boolean isActive() {
    return active;
  }

  @Override
  public void run() {
    try {
      while (active) {
        try {
          // listen for and accept a client connection to serverSocket
          final Socket socket = serverSocket.accept();

          socket.setPerformancePreferences(0, 2, 1);
          if (socketBufferSize > 0) {
            socket.setSendBufferSize(socketBufferSize);
            socket.setReceiveBufferSize(socketBufferSize);
          }
          // CREATE A NEW PROTOCOL INSTANCE
          final NetworkProtocolBinary connection = new NetworkProtocolBinary(this);
          connection.start();

        } catch (Exception e) {
          if (active)
            LogManager.instance().error(this, "Error on client connection", e);
        }
      }
    } finally {
      try {
        if (serverSocket != null && !serverSocket.isClosed())
          serverSocket.close();
      } catch (IOException ioe) {
      }
    }
  }

  public InetSocketAddress getInboundAddr() {
    return inboundAddr;
  }

  public String getListeningAddress(final boolean resolveMultiIfcWithLocal) {
    String address = serverSocket.getInetAddress().getHostAddress();
    if (resolveMultiIfcWithLocal && address.equals("0.0.0.0")) {
      try {
        address = Channel.getLocalIpAddress(true);
      } catch (Exception ex) {
        address = null;
      }
      if (address == null) {
        try {
          address = InetAddress.getLocalHost().getHostAddress();
        } catch (UnknownHostException e) {
          LogManager.instance().warn(this, "Error resolving current host address", e);
        }
      }
    }

    return address + ":" + serverSocket.getLocalPort();
  }

  public String getLocalHostIp() {
    try {
      InetAddress host = InetAddress.getLocalHost();
      InetAddress[] addrs = InetAddress.getAllByName(host.getHostName());
      for (InetAddress addr : addrs) {
        if (!addr.isLoopbackAddress()) {
          return addr.toString();
        }
      }
    } catch (UnknownHostException e) {
      try {
        return Channel.getLocalIpAddress(true);
      } catch (SocketException e1) {
        LogManager.instance().warn(this, "Error resolving localhost address", e1);
      }
    }
    return null;
  }

  @Override
  public String toString() {
    return serverSocket.getLocalSocketAddress().toString();
  }

  /**
   * Initialize a server socket for communicating with the client.
   *
   * @param iHostPortRange
   * @param iHostName
   */
  private void listen(final String iHostName, final String iHostPortRange) {

    for (int port : getPorts(iHostPortRange)) {
      inboundAddr = new InetSocketAddress(iHostName, port);
      try {
        serverSocket = socketFactory.createServerSocket(port, 0, InetAddress.getByName(iHostName));

        if (serverSocket.isBound()) {
          LogManager.instance().info(this,
              "Listening Replication connections on $ANSI{green " + inboundAddr.getAddress().getHostAddress() + ":" + inboundAddr
                  .getPort() + "} (protocol v." + protocolVersion + ")");

          return;
        }
      } catch (BindException be) {
        LogManager.instance().warn(this, "Port %s:%d busy, trying the next available...", iHostName, port);
      } catch (SocketException se) {
        LogManager.instance().error(this, "Unable to create socket", se);
        throw new RuntimeException(se);
      } catch (IOException ioe) {
        LogManager.instance().error(this, "Unable to read data from an open socket", ioe);
        System.err.println("Unable to read data from an open socket.");
        throw new RuntimeException(ioe);
      }
    }

    LogManager.instance()
        .error(this, "Unable to listen for connections using the configured ports '%s' on host '%s'", null, iHostPortRange,
            iHostName);

    throw new ServerException("Unable to listen for connections using the configured ports '%s' on host '%s'");
  }
}
