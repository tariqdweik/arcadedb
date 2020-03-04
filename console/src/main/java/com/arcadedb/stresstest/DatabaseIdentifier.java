/*
 * Copyright (c) - Arcade Data LTD (https://arcadedata.com)
 */
package com.arcadedb.stresstest;

import com.arcadedb.database.EmbeddedDatabase;
import com.arcadedb.remote.RemoteDatabase;

import java.io.File;

/**
 * A class that contains all the info needed to access a database
 */
public class DatabaseIdentifier {
  private StressTesterSettings settings;
  private EmbeddedDatabase     embeddedDatabase;
  private RemoteDatabase       remoteDatabase;

  public DatabaseIdentifier(final StressTesterSettings settings) {
    this.settings = settings;
  }

  public String getUrl() {
    switch (settings.mode) {
    case REMOTE:
      return "remote:" + settings.remoteIp + ":" + settings.remotePort + "/" + settings.dbName;
    case HA:
      return null;
    case EMBEDDED:
    default:
      String basePath = System.getProperty("java.io.tmpdir");
      if (settings.embeddedPath != null) {
        basePath = settings.embeddedPath;
      }

      if (!basePath.endsWith(File.separator))
        basePath += File.separator;

      return basePath + settings.dbName;
    }
  }

  public StressTester.OMode getMode() {
    return settings.mode;
  }

  public String getPassword() {
    return settings.rootPassword;
  }

  public void setPassword(String password) {
    settings.rootPassword = password;
  }

  public String getName() {
    return settings.dbName;
  }

  public String getRemoteIp() {
    return settings.remoteIp;
  }

  public int getRemotePort() {
    return settings.remotePort;
  }

  public String getEmbeddedPath() {
    return settings.embeddedPath;
  }

  public void drop() {
    if (embeddedDatabase != null)
      embeddedDatabase.drop();
    else if (remoteDatabase != null)
      remoteDatabase.drop();
  }

  public EmbeddedDatabase getEmbeddedDatabase() {
    return embeddedDatabase;
  }

  public void setEmbeddedDatabase(final EmbeddedDatabase embeddedDatabase) {
    this.embeddedDatabase = embeddedDatabase;
  }
}
