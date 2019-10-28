/*
 * Copyright (c) - Arcade Analytics LTD (https://arcadeanalytics.com)
 */

package com.arcadedb.server.http.handler;

import com.arcadedb.database.Database;
import com.arcadedb.server.ServerMetrics;
import com.arcadedb.server.http.HttpServer;
import com.arcadedb.sql.executor.ResultSet;
import io.undertow.server.HttpServerExchange;

import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.util.Deque;

public class GetQueryHandler extends DatabaseAbstractHandler {
  public GetQueryHandler(final HttpServer httpServer) {
    super(httpServer);
  }

  @Override
  public void execute(final HttpServerExchange exchange, final Database database) throws UnsupportedEncodingException {
    final Deque<String> text = exchange.getQueryParameters().get("command");
    if (text == null || text.isEmpty()) {
      exchange.setStatusCode(400);
      exchange.getResponseSender().send("{ \"error\" : \"Command text is null\"}");
      return;
    }

    final Deque<String> language = exchange.getQueryParameters().get("language");
    if (language == null || language.isEmpty()) {
      exchange.setStatusCode(400);
      exchange.getResponseSender().send("{ \"error\" : \"Language is null\"}");
      return;
    }

    final StringBuilder result = new StringBuilder();

    final ServerMetrics.MetricTimer timer = httpServer.getServer().getServerMetrics().timer("http.query");
    try {

      final String command = URLDecoder.decode(text.getFirst(), exchange.getRequestCharset());
      final ResultSet qResult = database.query(language.getFirst(), command);
      while (qResult.hasNext()) {
        if (result.length() > 0)
          result.append(",");
        result.append(httpServer.getJsonSerializer().serializeResult(qResult.next()).toString());
      }

    } finally {
      timer.stop();
    }

    exchange.setStatusCode(200);
    exchange.getResponseSender().send("{ \"result\" : [" + result.toString() + "] }");
  }
}