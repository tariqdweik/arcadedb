/*
 * Copyright (c) 2018 - Arcade Analytics LTD (https://arcadeanalytics.com)
 */

package com.arcadedb.server.http.handler;

import com.arcadedb.database.Database;
import com.arcadedb.server.http.HttpServer;
import io.undertow.server.HttpServerExchange;

import java.util.Deque;

public abstract class DatabaseAbstractHandler extends AbstractHandler {
  public DatabaseAbstractHandler(final HttpServer httpServer) {
    super(httpServer);
  }

  protected abstract void execute(HttpServerExchange exchange, Database database) throws Exception;

  @Override
  public void execute(HttpServerExchange exchange) throws Exception {
    final Deque<String> databaseName = exchange.getQueryParameters().get("database");
    if (databaseName.isEmpty()) {
      exchange.setStatusCode(400);
      exchange.getResponseSender().send("{ \"error\" : \"database is null\"}");
      return;
    }

    execute(exchange, httpServer.getUndertow().getDatabase(databaseName.getFirst()));
  }
}