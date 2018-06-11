/*
 * Copyright (c) 2018 - Arcade Analytics LTD (https://arcadeanalytics.com)
 */

package com.arcadedb.server.http.handler;

import com.arcadedb.database.Database;
import com.arcadedb.server.http.HttpServer;
import io.undertow.server.HttpServerExchange;
import io.undertow.util.HeaderValues;

import java.util.Base64;
import java.util.Deque;

public abstract class DatabaseAbstractHandler extends AbstractHandler {
  private static final String AUTHORIZATION_BASIC = "Basic";

  public DatabaseAbstractHandler(final HttpServer httpServer) {
    super(httpServer);
  }

  protected abstract void execute(HttpServerExchange exchange, Database database) throws Exception;

  @Override
  public void execute(HttpServerExchange exchange) throws Exception {
    final HeaderValues authorization = exchange.getRequestHeaders().get("Authorization");
    if (authorization == null || authorization.isEmpty()) {
      exchange.setStatusCode(403);
      exchange.getResponseSender().send("{ \"error\" : \"No authentication was provided\"}");
      return;
    }

    final String auth = authorization.getFirst();

    if (!auth.startsWith(AUTHORIZATION_BASIC)) {
      exchange.setStatusCode(403);
      exchange.getResponseSender().send("{ \"error\" : \"Authentication not supported\"}");
      return;
    }

    final String authPairCypher = auth.substring(AUTHORIZATION_BASIC.length() + 1);

    final String authPairClear = new String(Base64.getDecoder().decode(authPairCypher));

    final String[] authPair = authPairClear.split(":");

    if (authPair.length != 2) {
      exchange.setStatusCode(403);
      exchange.getResponseSender().send("{ \"error\" : \"Basic authentication error\"}");
      return;
    }

    authenticate(authPair[0], authPair[1]);

    final Deque<String> databaseName = exchange.getQueryParameters().get("database");
    if (databaseName.isEmpty()) {
      exchange.setStatusCode(400);
      exchange.getResponseSender().send("{ \"error\" : \"Database parameter is null\"}");
      return;
    }

    final Database db = httpServer.getServer().getDatabase(databaseName.getFirst());

    execute(exchange, db);
  }

  private void authenticate(final String userName, final String userPassword) {
    httpServer.getServer().getSecurity().authenticate(userName, userPassword);
  }
}