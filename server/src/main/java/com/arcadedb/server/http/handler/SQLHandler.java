/*
 * Copyright (c) 2018 - Arcade Analytics LTD (https://arcadeanalytics.com)
 */

package com.arcadedb.server.http.handler;

import com.arcadedb.database.Database;
import com.arcadedb.server.http.HttpServer;
import com.arcadedb.sql.executor.ResultSet;
import io.undertow.server.HttpServerExchange;

import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.util.Deque;

public class SQLHandler extends DatabaseAbstractHandler {
  public SQLHandler(final HttpServer httpServer) {
    super(httpServer);
  }

  @Override
  public void execute(final HttpServerExchange exchange, final Database database) throws UnsupportedEncodingException {
    final Deque<String> text = exchange.getQueryParameters().get("command");
    if (text == null || text.isEmpty()) {
      exchange.setStatusCode(400);
      exchange.getResponseSender().send("{ \"error\" : \"Command text id is null\"}");
      return;
    }

    final StringBuilder result = new StringBuilder();

    final String command = URLDecoder.decode(text.getFirst(), exchange.getRequestCharset());
    final ResultSet qResult = database.sql(command);
    while (qResult.hasNext()) {
      if (result.length() > 0)
        result.append(",");
      result.append(httpServer.getJsonSerializer().serializeResult(qResult.next()).toString());
    }

    exchange.setStatusCode(200);
    exchange.getResponseSender().send("{ \"result\" : [" + result.toString() + "] }");
  }
}