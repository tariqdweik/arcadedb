/*
 * Copyright (c) 2018 - Arcade Analytics LTD (https://arcadeanalytics.com)
 */

package com.arcadedb.server.handler;

import com.arcadedb.database.Database;
import com.arcadedb.server.HttpServer;
import io.undertow.server.HttpHandler;
import io.undertow.server.HttpServerExchange;
import io.undertow.util.Headers;
import org.xnio.Pooled;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.Deque;

public abstract class BasicHandler implements HttpHandler {
  protected final HttpServer httpServer;

  public BasicHandler(final HttpServer httpServer) {
    this.httpServer = httpServer;
  }

  protected abstract void execute(HttpServerExchange exchange, Database database) throws Exception;

  protected String parseRequestPayload(final HttpServerExchange exchange) throws IOException {
    final Pooled<ByteBuffer> pooledByteBuffer = exchange.getConnection().getBufferPool().allocate();
    final ByteBuffer byteBuffer = pooledByteBuffer.getResource();

    byteBuffer.clear();

    exchange.getRequestChannel().read(byteBuffer);
    final int pos = byteBuffer.position();
    byteBuffer.rewind();
    final byte[] bytes = new byte[pos];
    byteBuffer.get(bytes);

    final String requestBody = new String(bytes, Charset.forName("UTF-8"));

    byteBuffer.clear();
    pooledByteBuffer.free();

    return requestBody;
  }

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