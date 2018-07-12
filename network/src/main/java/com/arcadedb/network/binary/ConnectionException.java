/*
 * Copyright (c) 2018 - Arcade Analytics LTD (https://arcadeanalytics.com)
 */
package com.arcadedb.network.binary;

public class ConnectionException extends RuntimeException {
  private final String url;
  private final String reason;

  public ConnectionException(final String url, final Throwable e) {
    super("Error on connecting to server '" + url + "' (cause=" + e.toString() + ")", e);
    this.url = url;
    this.reason = e.toString();
  }

  public ConnectionException(final String url, final String reason) {
    super("Error on connecting to server '" + url + "' (cause=" + reason + ")");
    this.url = url;
    this.reason = reason;
  }
}
