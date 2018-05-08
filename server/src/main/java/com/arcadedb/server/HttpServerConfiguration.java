/*
 * Copyright (c) 2018 - Arcade Analytics LTD (https://arcadeanalytics.com)
 */

package com.arcadedb.server;

public class HttpServerConfiguration {
  public String databaseDirectory = "./database";
  public String bindServer        = "localhost";
  public int    bindPort          = 2480;

  public static HttpServerConfiguration create() {
    HttpServerConfiguration config = new HttpServerConfiguration();

    String prop = System.getProperty("server.configuration.databaseDirectory");
    if (prop != null)
      config.databaseDirectory = prop;

    prop = System.getProperty("server.configuration.bindServer");
    if (prop != null)
      config.bindServer = prop;

    prop = System.getProperty("server.configuration.bindPort");
    if (prop != null)
      config.bindPort = Integer.parseInt(prop);

    return config;
  }
}
