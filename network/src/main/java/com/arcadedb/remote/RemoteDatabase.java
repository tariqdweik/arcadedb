/*
 * Copyright (c) 2018 - Arcade Analytics LTD (https://arcadeanalytics.com)
 */

package com.arcadedb.remote;

import com.arcadedb.sql.executor.InternalResultSet;
import com.arcadedb.sql.executor.ResultInternal;
import com.arcadedb.sql.executor.ResultSet;
import com.arcadedb.utility.FileUtils;
import com.arcadedb.utility.LogManager;
import com.arcadedb.utility.Pair;
import com.arcadedb.utility.RWLockContext;
import org.json.JSONArray;
import org.json.JSONObject;

import java.io.DataOutputStream;
import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.*;

public class RemoteDatabase extends RWLockContext {

  public enum CONNECTION_STRATEGY {
    STICKY, ROUND_ROBIN
  }

  public interface Callback {
    Object call(HttpURLConnection iArgument, JSONObject response) throws Exception;
  }

  public static final int DEFAULT_PORT = 2480;

  private final String                      originalServer;
  private final int                         originalPort;
  private       String                      currentServer;
  private       int                         currentPort;
  private final String                      name;
  private final String                      userName;
  private final String                      userPassword;
  private       CONNECTION_STRATEGY         connectionStrategy        = CONNECTION_STRATEGY.ROUND_ROBIN;
  private       Pair<String, Integer>       leaderServer;
  private final List<Pair<String, Integer>> replicaServerList         = new ArrayList<>();
  private       int                         currentReplicaServerIndex = -1;

  private String protocol = "http";
  private String charset  = "UTF-8";

  public RemoteDatabase(final String server, final int port, final String name, final String userName, final String userPassword) {
    this.originalServer = server;
    this.originalPort = port;

    this.currentServer = originalServer;
    this.currentPort = originalPort;

    this.name = name;
    this.userName = userName;
    this.userPassword = userPassword;

    requestClusterConfiguration();
  }

  public void create() {
    databaseCommand("create", null, null, true, null);
  }

  public void close() {
  }

  public void drop() {
    databaseCommand("drop", null, null, true, null);
    close();
  }

  public ResultSet sql(final String command, final Object... args) {
    Map<String, Object> params = null;
    if (args != null && args.length > 0) {
      if (args.length == 1 && args[0] instanceof Map)
        params = (Map<String, Object>) args[0];
      else {
        params = new HashMap<>();
        for (Object o : args) {
          params.put("" + params.size(), o);
        }
      }
    }

    return (ResultSet) databaseCommand("sql", command, params, true, new Callback() {
      @Override
      public Object call(final HttpURLConnection connection, final JSONObject response) {
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

  public ResultSet query(final String command, final Object... args) {
    Map<String, Object> params = null;
    if (args != null && args.length > 0) {
      if (args.length == 1 && args[0] instanceof Map)
        params = (Map<String, Object>) args[0];
      else {
        params = new HashMap<>();
        for (Object o : args) {
          params.put("" + params.size(), o);
        }
      }
    }

    return (ResultSet) databaseCommand("query", command, params, false, new Callback() {
      @Override
      public Object call(final HttpURLConnection connection, final JSONObject response) {
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

  public void begin() {
    sql("begin");
  }

  public void commit() {
    sql("commit");
  }

  public void rollback() {
    sql("rollback");
  }

  public CONNECTION_STRATEGY getConnectionStrategy() {
    return connectionStrategy;
  }

  public void setConnectionStrategy(CONNECTION_STRATEGY connectionStrategy) {
    this.connectionStrategy = connectionStrategy;
  }

  @Override
  public String toString() {
    return name;
  }

  private Object serverCommand(final String operation, final String payloadCommand, final Map<String, Object> params, final boolean requiresLeader,
      final Callback callback) {
    return httpCommand(null, operation, payloadCommand, params, requiresLeader, callback);
  }

  private Object databaseCommand(final String operation, final String payloadCommand, final Map<String, Object> params, final boolean requiresLeader,
      final Callback callback) {
    return httpCommand(name, operation, payloadCommand, params, requiresLeader, callback);
  }

  private Object httpCommand(final String extendedURL, final String operation, final String payloadCommand, final Map<String, Object> params,
      final boolean requiresLeader, final Callback callback) {

    Exception lastException = null;

    final int maxRetry = requiresLeader ? 3 : replicaServerList.size() + 1;

    Pair<String, Integer> connectToServer = requiresLeader ? leaderServer : new Pair<>(currentServer, currentPort);

    for (int retry = 0; retry < maxRetry; ++retry) {
      String url = protocol + "://" + connectToServer.getFirst() + ":" + connectToServer.getSecond() + "/" + operation;

      if (extendedURL != null)
        url += "/" + extendedURL;

      try {
        final HttpURLConnection connection = connect(url);
        try {

          if (payloadCommand != null) {
            final JSONObject jsonRequest = new JSONObject();
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

          connection.connect();

          if (connection.getResponseCode() != 200) {
            String detail = "?";
            String reason = "?";
            String responsePayload = null;
            try {
              responsePayload = FileUtils.readStreamAsString(connection.getInputStream(), charset);
              final JSONObject response = new JSONObject(responsePayload);
              reason = response.getString("error");
              detail = response.getString("detail");
            } catch (Exception e) {
              lastException = e;
              LogManager.instance().warn(this, "Bad payload received, retrying... (payload=%s, error=%s)", responsePayload, e.toString());
              continue;
            }

            String cmd = payloadCommand;
            if (cmd == null)
              cmd = "-";

            throw new RemoteException(
                "Error on executing remote command '" + cmd + "' (httpErrorCode=" + connection.getResponseCode() + " httpErrorDescription=" + connection
                    .getResponseMessage() + " reason=" + reason + " detail=" + detail + ")");
          }

          final JSONObject response = new JSONObject(FileUtils.readStreamAsString(connection.getInputStream(), charset));

          if (callback == null)
            return null;

          return callback.call(connection, response);

        } finally {
          connection.disconnect();
        }

      } catch (RemoteException e) {
        throw e;
      } catch (IOException e) {
        lastException = e;

        if (!reloadClusterConfiguration())
          throw new RemoteException("Error on executing remote operation " + operation + ", no server available", e);

        if (requiresLeader) {
          connectToServer = leaderServer;
        } else
          connectToServer = getNextReplicaAddress();

        if (connectToServer == null)
          LogManager.instance()
              .warn(this, "Remote server seems unreachable, switching to server %s:%d...", connectToServer.getFirst(), connectToServer.getSecond());

      } catch (Exception e) {
        throw new RemoteException("Error on executing remote operation " + operation, e);
      }
    }

    throw new RemoteException("Error on executing remote operation " + operation, lastException);
  }

  protected HttpURLConnection connect(final String url) throws IOException {
    final HttpURLConnection connection = (HttpURLConnection) new URL(url).openConnection();
    connection.setRequestProperty("charset", "utf-8");
    connection.setRequestMethod("POST");

    final String authorization = userName + ":" + userPassword;
    connection.setRequestProperty("Authorization", "Basic " + Base64.getEncoder().encodeToString(authorization.getBytes()));

    connection.setDoOutput(true);
    return connection;
  }

  private void requestClusterConfiguration() {
    serverCommand("server", null, null, false, new Callback() {
      @Override
      public Object call(final HttpURLConnection connection, final JSONObject response) {
        LogManager.instance().info(this, "Configuring remote database: %s", response);

        if (!response.has("leaderServer")) {
          leaderServer = new Pair<>(originalServer, originalPort);
          replicaServerList.clear();
          return null;
        }

        final String cfgLeaderServer = (String) response.get("leaderServer");
        final String[] leaderServerParts = cfgLeaderServer.split(":");
        leaderServer = new Pair<>(leaderServerParts[0], Integer.parseInt(leaderServerParts[1]));

        final String cfgReplicaServers = (String) response.get("replicaServers");

        // PARSE SERVER LISTS
        replicaServerList.clear();

        if (cfgReplicaServers != null && !cfgReplicaServers.isEmpty()) {
          final String[] serverEntries = cfgReplicaServers.split(",");
          for (String serverEntry : serverEntries) {
            final String[] serverParts = serverEntry.split(":");

            final String sHost = serverParts[0];
            final int sPort = Integer.parseInt(serverParts[1]);

            replicaServerList.add(new Pair(sHost, sPort));
          }
        }

        LogManager.instance().info(this, "Remote Database configured with leader=%s and replicas=%s", leaderServer, replicaServerList);

        return null;
      }
    });
  }

  private Pair<String, Integer> getNextReplicaAddress() {
    if (replicaServerList.isEmpty())
      return leaderServer;

    ++currentReplicaServerIndex;
    if (currentReplicaServerIndex > replicaServerList.size() - 1)
      currentReplicaServerIndex = 0;

    return replicaServerList.get(currentReplicaServerIndex);
  }

  private boolean reloadClusterConfiguration() {
    final Pair<String, Integer> oldLeader = leaderServer;

    // ASK TO REPLICA FIRST
    for (int replicaIdx = 0; replicaIdx < replicaServerList.size(); ++replicaIdx) {
      Pair<String, Integer> connectToServer = replicaServerList.get(replicaIdx);

      currentServer = connectToServer.getFirst();
      currentPort = connectToServer.getSecond();
      requestClusterConfiguration();

      if (leaderServer != null)
        return true;
    }

    if (oldLeader != null) {
      // ASK TO THE OLD LEADER
      currentServer = oldLeader.getFirst();
      currentPort = oldLeader.getSecond();
      requestClusterConfiguration();
    }

    return leaderServer != null;
  }
}