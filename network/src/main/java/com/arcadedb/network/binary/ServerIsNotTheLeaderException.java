/*
 * Copyright (c) 2018 - Arcade Analytics LTD (https://arcadeanalytics.com)
 */
package com.arcadedb.network.binary;

public class ServerIsNotTheLeaderException extends RuntimeException {

  private final String leaderURL;

  public ServerIsNotTheLeaderException(final String message, final String leaderURL) {
    super(message);
    this.leaderURL = leaderURL;
  }

  public String getLeaderAddress() {
    return leaderURL;
  }

  @Override
  public String toString() {
    return super.toString() + ". The leader is server " + leaderURL;
  }
}
