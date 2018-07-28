/*
 * Copyright (c) 2018 - Arcade Analytics LTD (https://arcadeanalytics.com)
 */

package com.arcadedb.server;

import com.arcadedb.utility.LogManager;
import org.json.JSONObject;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.DataOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.HashMap;
import java.util.Map;

public class HTTPGraphTest extends BaseGraphServerTest {
  @Test
  public void checkAuthenticationError() throws IOException {
    HttpURLConnection connection = (HttpURLConnection) new URL("http://127.0.0.1:2480/query/graph/sql/select%20from%20V1%20limit%201").openConnection();

    connection.setRequestMethod("GET");
    connection.setRequestProperty("Authorization", "Basic " + Base64.getEncoder().encodeToString("root:wrong".getBytes()));

    try {
      connection.connect();

      readResponse(connection);

      Assertions.fail("Authentication was bypassed!");

    } catch (IOException e) {
      Assertions.assertTrue(e.toString().contains("403"));
    } finally {
      connection.disconnect();
    }
  }

  @Test
  public void checkNoAuthentication() throws IOException {
    HttpURLConnection connection = (HttpURLConnection) new URL("http://127.0.0.1:2480/query/graph/sql/select%20from%20V1%20limit%201").openConnection();

    connection.setRequestMethod("GET");

    try {
      connection.connect();

      readResponse(connection);

      Assertions.fail("Authentication was bypassed!");

    } catch (IOException e) {
      Assertions.assertTrue(e.toString().contains("403"));
    } finally {
      connection.disconnect();
    }
  }

  @Test
  public void checkQueryInGet() throws IOException {
    HttpURLConnection connection = (HttpURLConnection) new URL("http://127.0.0.1:2480/query/graph/sql/select%20from%20V1%20limit%201").openConnection();

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
  public void checkQueryInPost() throws IOException {
    HttpURLConnection connection = (HttpURLConnection) new URL("http://127.0.0.1:2480/query/graph").openConnection();

    connection.setRequestMethod("POST");
    connection.setRequestProperty("Authorization", "Basic " + Base64.getEncoder().encodeToString("root:root".getBytes()));
    formatPost(connection, "sql", "select from V1 limit 1", new HashMap<>());
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
  public void checkCommand() throws IOException {
    HttpURLConnection connection = (HttpURLConnection) new URL("http://127.0.0.1:2480/command/graph").openConnection();

    connection.setRequestMethod("POST");
    connection.setRequestProperty("Authorization", "Basic " + Base64.getEncoder().encodeToString("root:root".getBytes()));
    formatPost(connection, "sql", "select from V1 limit 1", new HashMap<>());
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
    connection.setRequestProperty("Authorization", "Basic " + Base64.getEncoder().encodeToString("root:root".getBytes()));

    final String payload = "{\"@type\":\"Person\",\"name\":\"Jay\",\"surname\":\"Miner\",\"age\":69}";

    connection.setRequestMethod("POST");
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

  private void formatPost(final HttpURLConnection connection, final String language, final String payloadCommand, final Map<String, Object> params) throws IOException {
    connection.setDoOutput(true);
    if (payloadCommand != null) {
      final JSONObject jsonRequest = new JSONObject();
      jsonRequest.put("language", language);
      jsonRequest.put("command", payloadCommand);

      if (params != null) {
        final JSONObject jsonParams = new JSONObject(params);
        jsonRequest.put("params", jsonParams);
      }

      final byte[] postData = jsonRequest.toString().getBytes(StandardCharsets.UTF_8);
      connection.setRequestProperty("Content-Length", Integer.toString(postData.length));
      try (DataOutputStream wr = new DataOutputStream(connection.getOutputStream())) {
        wr.write(postData);
      }
    }
  }
}