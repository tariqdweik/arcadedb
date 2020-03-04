/*
 * Copyright (c) - Arcade Data LTD (https://arcadedata.com)
 */
package com.arcadedb.network.binary;

public class NetworkProtocolException extends RuntimeException {

  public NetworkProtocolException(String string) {
    super(string);
  }

  public NetworkProtocolException(String string, Exception exception) {
    super(string, exception);
  }
}
