/*
 * Copyright (c) 2018 - Arcade Analytics LTD (https://arcadeanalytics.com)
 */
package com.arcadedb.network.binary;

public class ServerIsNotTheLeaderException extends RuntimeException {

  private final String leaderURL;

  public ServerIsNotTheLeaderException(String string, String leaderURL) {
    super(string);
    this.leaderURL = leaderURL;
  }

  public String getLeaderURL() {
    return leaderURL;
  }
}
