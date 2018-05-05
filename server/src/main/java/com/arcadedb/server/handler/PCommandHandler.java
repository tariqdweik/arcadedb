package com.arcadedb.server.handler;

import com.arcadedb.database.PDatabase;
import com.arcadedb.server.PHttpServer;
import com.arcadedb.sql.executor.OResultSet;
import io.undertow.server.HttpServerExchange;

import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.util.Deque;

public class PCommandHandler extends PBasicHandler {
  public PCommandHandler(final PHttpServer pHttpServer) {
    super(pHttpServer);
  }

  @Override
  public void execute(final HttpServerExchange exchange, final PDatabase database) throws UnsupportedEncodingException {
    final Deque<String> text = exchange.getQueryParameters().get("command");
    if (text.isEmpty()) {
      exchange.setStatusCode(400);
      exchange.getResponseSender().send("{ \"error\" : \"Query text id is null\"}");
      return;
    }

    final String command = URLDecoder.decode(text.getFirst(), exchange.getRequestCharset());

    final OResultSet qResult = database.command(command, null);

    final StringBuilder result = new StringBuilder();
    while (qResult.hasNext()) {
      if (result.length() > 0)
        result.append(",");
      result.append(httpServer.getJsonSerializer().serializeResult(qResult.next()).toString());
    }

    exchange.setStatusCode(200);
    exchange.getResponseSender().send("{ \"result\" : [" + result.toString() + "] }");
  }
}