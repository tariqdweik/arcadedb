/*
 * Copyright (c) 2018 - Arcade Analytics LTD (https://arcadeanalytics.com)
 */

package com.arcadedb.remote;

import com.arcadedb.sql.executor.InternalResultSet;
import com.arcadedb.sql.executor.ResultInternal;
import com.arcadedb.sql.executor.ResultSet;
import com.arcadedb.utility.FileUtils;
import com.arcadedb.utility.RWLockContext;
import org.json.JSONArray;
import org.json.JSONObject;

import java.io.IOException;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.net.URLEncoder;
import java.util.Base64;
import java.util.Scanner;

public class RemoteDatabase extends RWLockContext {
  public enum CONNECTION_STRATEGY {
    STICKY, ROUND_ROBIN_CONNECT, ROUND_ROBIN_REQUEST
  }

  public interface Callback {
    Object call(HttpURLConnection iArgument, JSONObject response) throws Exception;
  }

  public static final int DEFAULT_PORT = 2480;

  private final String server;
  private final int    port;
  private final String name;
  private final String userName;
  private final String userPassword;
  private       String protocol = "http";
  private       String charset  = "UTF-8";

  public RemoteDatabase(final String server, final int port, final String name, final String userName, final String userPassword) {
    this.server = server;
    this.port = port;
    this.name = name;
    this.userName = userName;
    this.userPassword = userPassword;
  }

  public void create() {
    connect("create", null, new Callback() {
      @Override
      public Object call(final HttpURLConnection connection, final JSONObject response) {
        return null;
      }
    });
  }

  public void close() {
  }

  public void drop() {
    connect("drop", null, new Callback() {
      @Override
      public Object call(final HttpURLConnection connection, final JSONObject response) {
        return null;
      }
    });
  }

  public ResultSet command(final String command) {
    return (ResultSet) connect("sql", command, new Callback() {
      @Override
      public Object call(final HttpURLConnection connection, final JSONObject response) throws Exception {
        final ResultSet resultSet = new InternalResultSet();

        final JSONArray resultArray = response.getJSONArray("result");
        for (int i = 0; i < resultArray.length(); ++i) {
          final JSONObject result = resultArray.getJSONObject(i);
          ((InternalResultSet) resultSet).add(new ResultInternal(result.toMap()));
        }

        return resultSet;
      }
    });
  }

  @Override
  public String toString() {
    return name;
  }

  private Object connect(final String operation, final String command, final Callback callback) {
    final HttpURLConnection connection;
    try {
      String url = protocol + "://" + server + ":" + port + "/" + operation + "/" + name;

      if (command != null)
        url += "/" + URLEncoder.encode(command, charset);

      connection = (HttpURLConnection) new URL(url).openConnection();
      try {
        connection.setRequestMethod("POST");

        final String authorization = userName + ":" + userPassword;
        connection.setRequestProperty("Authorization", "Basic " + Base64.getEncoder().encodeToString(authorization.getBytes()));

        connection.connect();

        if (connection.getResponseCode() != 200) {
          String detail = "?";
          String reason = "?";
          try {
            final JSONObject response = new JSONObject(FileUtils.readStreamAsString(connection.getInputStream(), charset));
            reason = response.getString("error");
            detail = response.getString("detail");
          } catch (Exception e) {
          }

          throw new RemoteException(
              "Error executing remote command '" + command + "' (httpErrorCode=" + connection.getResponseCode()
                  + " httpErrorDescription=" + connection.getResponseMessage() + " reason=" + reason + " detail=" + detail + ")");
        }

        final JSONObject response = new JSONObject(FileUtils.readStreamAsString(connection.getInputStream(), charset));

        return callback.call(connection, response);

      } finally {
        connection.disconnect();
      }
    } catch (RemoteException e) {
      throw e;
    } catch (Exception e) {
      throw new RemoteException("Error on executing remote operation " + operation, e);
    }
  }

  protected String readResponse(final HttpURLConnection connection) throws IOException {
    InputStream in = connection.getInputStream();
    Scanner scanner = new Scanner(in);

    final StringBuilder buffer = new StringBuilder();

    while (scanner.hasNext()) {
      buffer.append(scanner.next().replace('\n', ' '));
    }

    return buffer.toString();
  }
}