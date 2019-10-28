/*
 * Copyright (c) - Arcade Analytics LTD (https://arcadeanalytics.com)
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
