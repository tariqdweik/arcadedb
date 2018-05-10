/*
 * Copyright (c) 2018 - Arcade Analytics LTD (https://arcadeanalytics.com)
 */

package com.arcadedb.network.binary;

import java.io.*;
import java.net.Socket;

public class ChannelBinaryServer extends ChannelBinary {

  public ChannelBinaryServer(final Socket iSocket) throws IOException {
    super(iSocket);

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
