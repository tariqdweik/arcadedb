package com.arcadedb;

public class PConstants {
  public static final String URL       = "https://arcadeanalytics.com";
  public static final String COPYRIGHT = "Copyrights (c) 2018 Arcade Analytics";
  public static final String VERSION   = "0.1-SNAPSHOT";

  /**
   * Returns true if current version is a snapshot.
   */
  public static boolean isSnapshot() {
    return VERSION.endsWith("SNAPSHOT");
  }
}
