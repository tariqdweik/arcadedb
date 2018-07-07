/*
 * Copyright (c) 2018 - Arcade Analytics LTD (https://arcadeanalytics.com)
 */

package com.arcadedb.server.http.handler;

import com.arcadedb.server.ServerSecurityException;
import com.arcadedb.server.http.HttpServer;
import com.arcadedb.utility.LogManager;
import io.undertow.server.HttpHandler;
import io.undertow.server.HttpServerExchange;
import io.undertow.util.HeaderValues;
import io.undertow.util.Headers;

import java.io.IOException;
import java.util.Base64;

public abstract class AbstractHandler implements HttpHandler {
  private static final String AUTHORIZATION_BASIC = "Basic";

  protected final HttpServer httpServer;

  public AbstractHandler(final HttpServer httpServer) {
    this.httpServer = httpServer;
  }

  protected abstract void execute(HttpServerExchange exchange) throws Exception;

  protected String parseRequestPayload(final HttpServerExchange e) throws IOException {
    final StringBuilder result = new StringBuilder();
    e.getRequestReceiver().receiveFullBytes((exchange, data) -> {
      result.append(new String(data));
    });
    return result.toString();
  }

  @Override
  public void handleRequest(HttpServerExchange exchange) {
    LogManager.instance().setContext(httpServer.getServer().getServerName());

    try {
      exchange.getResponseHeaders().put(Headers.CONTENT_TYPE, "application/json");

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

      execute(exchange);

    } catch (ServerSecurityException e) {
      LogManager.instance().error(this, "Error on command execution (%s)", e, getClass().getSimpleName());
      exchange.setStatusCode(403);
      exchange.getResponseSender().send("{ \"error\" : \"Security error\", \"detail\":\"" + e.toString() + "\"}");
    } catch (Exception e) {
      LogManager.instance().error(this, "Error on command execution (%s)", e, getClass().getSimpleName());
      exchange.setStatusCode(500);
      exchange.getResponseSender().send("{ \"error\" : \"Internal error\", \"detail\":\"" + e.toString() + "\"}");
    } finally {
      LogManager.instance().setContext(null);
    }
  }

  protected void authenticate(final String userName, final String userPassword) {
    httpServer.getServer().getSecurity().authenticate(userName, userPassword);
  }
}