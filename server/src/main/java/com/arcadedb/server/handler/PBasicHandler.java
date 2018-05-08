package com.arcadedb.server.handler;

import com.arcadedb.database.Database;
import com.arcadedb.server.HttpServer;
import io.undertow.server.HttpHandler;
import io.undertow.server.HttpServerExchange;
import io.undertow.util.Headers;

import java.util.Deque;

public abstract class PBasicHandler implements HttpHandler {
  protected final HttpServer httpServer;

  public PBasicHandler(final HttpServer httpServer) {
    this.httpServer = httpServer;
  }

  protected abstract void execute(HttpServerExchange exchange, Database database) throws Exception;

  @Override
  public final void handleRequest(HttpServerExchange exchange) throws Exception {
    try {
      exchange.getResponseHeaders().put(Headers.CONTENT_TYPE, "application/json");

      final Deque<String> databaseName = exchange.getQueryParameters().get("database");
      if (databaseName.isEmpty()) {
        exchange.setStatusCode(400);
        exchange.getResponseSender().send("{ \"error\" : \"database is null\"}");
        return;
      }

      execute(exchange, httpServer.getDatabase(databaseName.getFirst()));

    } catch (Exception e) {
      exchange.setStatusCode(500);
      exchange.getResponseSender().send("{ \"error\" : \"Internal error\", \"detail\":\"" + e.toString() + "\"}");
    }
  }
}