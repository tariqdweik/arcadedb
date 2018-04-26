package com.arcadedb.server;

import com.arcadedb.database.PDatabase;
import com.arcadedb.exception.PRecordNotFoundException;
import com.arcadedb.sql.executor.OResultSet;
import io.undertow.server.HttpServerExchange;
import io.undertow.util.Headers;

import java.util.Deque;

public class PQueryHandler extends PBasicHandler {
  public PQueryHandler(final PHttpServer pHttpServer) {
    super(pHttpServer);
  }

  @Override
  public void handleRequest(HttpServerExchange exchange) throws Exception {
    try {
      exchange.getResponseHeaders().put(Headers.CONTENT_TYPE, "application/json");

      final Deque<String> databaseName = exchange.getQueryParameters().get("database");
      if (databaseName.isEmpty()) {
        exchange.getResponseSender().send("{ \"error\" : \"database is null\"}");
        return;
      }

      final Deque<String> text = exchange.getQueryParameters().get("text");
      if (text.isEmpty()) {
        exchange.setStatusCode(500);
        exchange.getResponseSender().send("{ \"error\" : \"Query text id is null\"}");
        return;
      }

      final PDatabase db = httpServer.getDatabase(databaseName.getFirst());

      final OResultSet qResult = db.query(text.getFirst(), null);

      final StringBuilder result = new StringBuilder();
      while (qResult.hasNext()) {
        if (result.length() > 0)
          result.append(",");
        result.append(httpServer.getJsonSerializer().serializeResult(qResult.next()).toString());
      }

      exchange.setStatusCode(200);
      exchange.getResponseSender().send("{ \"result\" : [" + result.toString() + "] }");

    } catch (PRecordNotFoundException e) {
      exchange.setStatusCode(404);
      exchange.getResponseSender().send("{ \"error\" : \"Record id is null\"}");
    } catch (Exception e) {
      exchange.setStatusCode(500);
      exchange.getResponseSender().send("{ \"error\" : \"Internal error\", \"detail\":\"" + e.toString() + "\"}");
    }
  }
}
