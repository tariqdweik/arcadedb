/*
 * Copyright (c) - Arcade Data LTD (https://arcadedata.com)
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
  public void execute(final HttpServerExchange exchange) throws Exception {
    final Database db;
    if (openDatabase()) {
      final Deque<String> databaseName = exchange.getQueryParameters().get("database");
      if (databaseName.isEmpty()) {
        exchange.setStatusCode(400);
        exchange.getResponseSender().send("{ \"error\" : \"Database parameter is null\"}");
        return;
      }

      db = httpServer.getServer().getDatabase(databaseName.getFirst());
    } else
      db = null;

    execute(exchange, db);
  }

  protected boolean openDatabase() {
    return true;
  }
}