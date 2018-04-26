package com.arcadedb.server;

import com.arcadedb.database.PDatabase;
import com.arcadedb.database.PDocument;
import com.arcadedb.database.PRID;
import com.arcadedb.exception.PRecordNotFoundException;
import io.undertow.server.HttpServerExchange;
import io.undertow.util.Headers;

import java.util.Deque;

public class PRecordHandler extends PBasicHandler {
  public PRecordHandler(final PHttpServer pHttpServer) {
    super(pHttpServer);
  }

  @Override
  public void handleRequest(final HttpServerExchange exchange) throws Exception {
    try {
      exchange.getResponseHeaders().put(Headers.CONTENT_TYPE, "application/json");

      final Deque<String> databaseName = exchange.getQueryParameters().get("database");
      if (databaseName.isEmpty()) {
        exchange.setStatusCode(500);
        exchange.getResponseSender().send("{ \"error\" : \"database is null\"}");
        return;
      }

      final Deque<String> rid = exchange.getQueryParameters().get("rid");
      if (rid.isEmpty()) {
        exchange.setStatusCode(500);
        exchange.getResponseSender().send("{ \"error\" : \"Record id is null\"}");
        return;
      }

      final PDatabase db = httpServer.getDatabase(databaseName.getFirst());

      final String[] ridParts = rid.getFirst().split(":");

      final PDocument record = (PDocument) db
          .lookupByRID(new PRID(db, Integer.parseInt(ridParts[0]), Long.parseLong(ridParts[1])), true);

      exchange.setStatusCode(200);
      exchange.getResponseSender()
          .send("{ \"result\" : " + httpServer.getJsonSerializer().serializeRecord(record).toString() + "}");

    } catch (PRecordNotFoundException e) {
      exchange.setStatusCode(404);
      exchange.getResponseSender().send("{ \"error\" : \"Record id is null\"}");
    } catch (Exception e) {
      exchange.setStatusCode(500);
      exchange.getResponseSender().send("{ \"error\" : \"Internal error\", \"detail\":\"" + e.toString() + "\"}");
    }
  }
}
