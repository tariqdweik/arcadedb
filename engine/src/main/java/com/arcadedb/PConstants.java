package com.arcadedb;

public class PConstants {
  public static final String URL       = "https://www.protondb.com";
  public static final String COPYRIGHT = "Copyrights (c) 2018 Proton";
  public static final String VERSION   = "0.1-SNAPSHOT";

  /**
   * Returns the complete text of the current version.
   */
  public static String getVersion() {
    final StringBuilder buffer = new StringBuilder();
    buffer.append(PConstants.VERSION);
    return buffer.toString();
  }

  /**
   * Returns true if current version is a snapshot.
   */
  public static boolean isSnapshot() {
    return VERSION.endsWith("SNAPSHOT");
  }
}
