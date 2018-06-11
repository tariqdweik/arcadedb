/*
 * Copyright (c) 2018 - Arcade Analytics LTD (https://arcadeanalytics.com)
 */

package com.arcadedb.server;

import com.arcadedb.utility.LogManager;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.Base64;

public class TwoServersTest extends BaseGraphServerTest {
  @Override
  protected int getServerCount() {
    return 2;
  }

  @Test
  public void checkQuery() throws IOException {
    HttpURLConnection connection = (HttpURLConnection) new URL("http://127.0.0.1:2480/sql/graph/select%20from%20V1%20limit%201")
        .openConnection();

    connection.setRequestMethod("POST");
    connection.setRequestProperty("Authorization", "Basic " + Base64.getEncoder().encodeToString("root:root".getBytes()));
    connection.connect();

    try {
      final String response = readResponse(connection);

      LogManager.instance().info(this, "Response: ", response);

      Assertions.assertEquals(200, connection.getResponseCode());

      Assertions.assertEquals("OK", connection.getResponseMessage());

      Assertions.assertTrue(response.contains("V1"));

    } finally {
      connection.disconnect();
    }
  }

  @Test
  public void checkRecordLoading() throws IOException {
    HttpURLConnection connection = (HttpURLConnection) new URL(
        "http://127.0.0.1:2480/document/graph/" + BaseGraphServerTest.root.getIdentity().toString().substring(1)).openConnection();

    connection.setRequestMethod("GET");
    connection.setRequestProperty("Authorization", "Basic " + Base64.getEncoder().encodeToString("root:root".getBytes()));
    connection.connect();

    try {
      final String response = readResponse(connection);

      LogManager.instance().info(this, "Response: ", response);

      Assertions.assertEquals(200, connection.getResponseCode());

      Assertions.assertEquals("OK", connection.getResponseMessage());

      Assertions.assertTrue(response.contains("V1"));

    } finally {
      connection.disconnect();
    }
  }

  @Test
  public void checkRecordCreate() throws IOException {
    HttpURLConnection connection = (HttpURLConnection) new URL("http://127.0.0.1:2480/document/graph").openConnection();

    connection.setRequestMethod("POST");

    final String payload = "{\"@type\":\"Person\",\"name\":\"Jay\",\"surname\":\"Miner\",\"age\":69}";

    connection.setRequestMethod("POST");
    connection.setRequestProperty("Authorization", "Basic " + Base64.getEncoder().encodeToString("root:root".getBytes()));
    connection.setDoOutput(true);

    connection.connect();

    PrintWriter pw = new PrintWriter(new OutputStreamWriter(connection.getOutputStream()));
    pw.write(payload);
    pw.close();

    try {
      final String response = readResponse(connection);

      Assertions.assertEquals(200, connection.getResponseCode());
      Assertions.assertEquals("OK", connection.getResponseMessage());

      LogManager.instance().info(this, "Response: ", response);

      Assertions.assertTrue(response.contains("#"));

    } finally {
      connection.disconnect();
    }
  }
}