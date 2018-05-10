/*
 * Copyright (c) 2018 - Arcade Analytics LTD (https://arcadeanalytics.com)
 */
package com.arcadedb.server.ha;

import com.arcadedb.Constants;
import com.arcadedb.network.binary.ChannelBinary;

public class NetworkProtocolBinary extends Thread {
  private final ServerNetworkListener listener;
  protected     ChannelBinary         channel;

  public NetworkProtocolBinary(final ServerNetworkListener listener) {
    this.listener = listener;
    setName(Constants.PRODUCT + " <- replication/?");
  }

}
