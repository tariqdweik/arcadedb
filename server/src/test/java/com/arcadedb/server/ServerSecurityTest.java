/*
 * Copyright (c) 2018 - Arcade Analytics LTD (https://arcadeanalytics.com)
 */

package com.arcadedb.server;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class ServerSecurityTest {

  @Test
  public void checkQuery() {
    final ServerSecurity security = new ServerSecurity(null, "./target");
    security.startService();

    Assertions
        .assertEquals("PBKDF2WithHmacSHA256$65536$ThisIsTheSalt$wIKUzWYH72cKJRnFZ0PTSevERtwZTNdN+W4/Fd7xBvw=", security.encode("ThisIsATest", "ThisIsTheSalt"));
    Assertions
        .assertEquals("PBKDF2WithHmacSHA256$65536$ThisIsTheSalt$wIKUzWYH72cKJRnFZ0PTSevERtwZTNdN+W4/Fd7xBvw=", security.encode("ThisIsATest", "ThisIsTheSalt"));

    for (int i = 0; i < 1000000; ++i) {
      Assertions.assertFalse(security.generateRandomSalt().contains("$"));
    }

    security.stopService();
  }

  private void passwordShouldMatch(final ServerSecurity security, String password, String expectedHash) {
    Assertions.assertTrue(security.checkPassword(password, expectedHash));
  }

  private void passwordShouldNotMatch(final ServerSecurity security, String password, String expectedHash) {
    Assertions.assertFalse(security.checkPassword(password, expectedHash));
  }
}