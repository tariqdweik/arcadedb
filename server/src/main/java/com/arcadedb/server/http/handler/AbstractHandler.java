/*
 * Copyright (c) 2018 - Arcade Analytics LTD (https://arcadeanalytics.com)
 */

package com.arcadedb.server.http.handler;

import com.arcadedb.server.ServerSecurityException;
import com.arcadedb.server.http.HttpServer;
import com.arcadedb.utility.LogManager;
import io.undertow.connector.PooledByteBuffer;
import io.undertow.server.HttpHandler;
import io.undertow.server.HttpServerExchange;
import io.undertow.util.Headers;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;

public abstract class AbstractHandler implements HttpHandler {
  protected final HttpServer httpServer;

  public AbstractHandler(final HttpServer httpServer) {
    this.httpServer = httpServer;
  }

  protected abstract void execute(HttpServerExchange exchange) throws Exception;

  protected String parseRequestPayload(final HttpServerExchange exchange) throws IOException {
    final PooledByteBuffer pooledByteBuffer = exchange.getConnection().getByteBufferPool().allocate();
    final ByteBuffer byteBuffer = pooledByteBuffer.getBuffer();

    byteBuffer.clear();

    exchange.getRequestChannel().read(byteBuffer);
    final int pos = byteBuffer.position();
    byteBuffer.rewind();

    final byte[] bytes = new byte[pos];
    byteBuffer.get(bytes);

    final String requestBody = new String(bytes, Charset.forName("UTF-8"));

    byteBuffer.clear();
    pooledByteBuffer.close();

    return requestBody;
  }

  @Override
  public void handleRequest(HttpServerExchange exchange) throws Exception {
    try {
      exchange.getResponseHeaders().put(Headers.CONTENT_TYPE, "application/json");

      execute(exchange);

    } catch (ServerSecurityException e) {
      LogManager.instance().error(this, "Error on command execution (%s)", e, getClass().getSimpleName());
      exchange.setStatusCode(403);
      exchange.getResponseSender().send("{ \"error\" : \"Security error\", \"detail\":\"" + e.toString() + "\"}");
    } catch (Exception e) {
      LogManager.instance().error(this, "Error on command execution (%s)", e, getClass().getSimpleName());
      exchange.setStatusCode(500);
      exchange.getResponseSender().send("{ \"error\" : \"Internal error\", \"detail\":\"" + e.toString() + "\"}");
    }
  }
}