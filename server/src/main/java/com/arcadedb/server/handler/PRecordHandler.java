package com.arcadedb.server.handler;

import com.arcadedb.database.PDatabase;
import com.arcadedb.database.PDocument;
import com.arcadedb.database.PRID;
import com.arcadedb.exception.PRecordNotFoundException;
import com.arcadedb.server.PHttpServer;
import io.undertow.server.HttpServerExchange;

import java.util.Deque;

public class PRecordHandler extends PBasicHandler {
  public PRecordHandler(final PHttpServer pHttpServer) {
    super(pHttpServer);
  }

  @Override
  public void execute(final HttpServerExchange exchange, final PDatabase database) {
    try {
      final Deque<String> rid = exchange.getQueryParameters().get("rid");
      if (rid.isEmpty()) {
        exchange.setStatusCode(400);
        exchange.getResponseSender().send("{ \"error\" : \"Record id is null\"}");
        return;
      }

      final String[] ridParts = rid.getFirst().split(":");

      final PDocument record = (PDocument) database
          .lookupByRID(new PRID(database, Integer.parseInt(ridParts[0]), Long.parseLong(ridParts[1])), true);

      exchange.setStatusCode(200);
      exchange.getResponseSender()
          .send("{ \"result\" : " + httpServer.getJsonSerializer().serializeRecord(record).toString() + "}");

    } catch (PRecordNotFoundException e) {
      exchange.setStatusCode(404);
      exchange.getResponseSender().send("{ \"error\" : \"Record id is null\"}");
    }
  }
}