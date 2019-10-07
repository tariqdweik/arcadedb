/*
 * Copyright (c) 2018 - Arcade Analytics LTD (https://arcadeanalytics.com)
 */

package com.arcadedb.network.binary;

import com.arcadedb.ContextConfiguration;
import com.arcadedb.GlobalConfiguration;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.Socket;

public class ChannelBinaryServer extends ChannelBinary {

  public ChannelBinaryServer(final Socket iSocket, final ContextConfiguration config) throws IOException {
    super(iSocket,config.getValueAsInteger(GlobalConfiguration.HA_REPLICATION_CHUNK_MAXSIZE));

    if (socketBufferSize > 0) {
      inStream = new BufferedInputStream(socket.getInputStream(), socketBufferSize);
      outStream = new BufferedOutputStream(socket.getOutputStream(), socketBufferSize);
    } else {
      inStream = new BufferedInputStream(socket.getInputStream());
      outStream = new BufferedOutputStream(socket.getOutputStream());
    }

    out = new DataOutputStream(outStream);
    in = new DataInputStream(inStream);
  }
}
