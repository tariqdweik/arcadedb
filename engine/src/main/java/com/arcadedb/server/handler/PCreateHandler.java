package com.arcadedb.server.handler;

import com.arcadedb.database.PDatabase;
import com.arcadedb.exception.PRecordNotFoundException;
import com.arcadedb.server.PHttpServer;
import io.undertow.server.HttpServerExchange;

import java.util.Deque;

public class PCreateHandler extends PBasicHandler {
  public PCreateHandler(final PHttpServer pHttpServer) {
    super(pHttpServer);
  }

  @Override
  public void execute(final HttpServerExchange exchange, final PDatabase database) {
    try {
      final Deque<String> recordInJson = exchange.getQueryParameters().get("record");
      if (recordInJson.isEmpty()) {
        exchange.setStatusCode(400);
        exchange.getResponseSender().send("{ \"error\" : \"Record is null\"}");
        return;
      }

//      final PDocument record =
//
//      exchange.setStatusCode(200);
//      exchange.getResponseSender()
//          .send("{ \"result\" : " + httpServer.getJsonSerializer().serializeRecord(record).toString() + "}");

    } catch (PRecordNotFoundException e) {
      exchange.setStatusCode(404);
      exchange.getResponseSender().send("{ \"error\" : \"Record id is null\"}");
    }
  }
}
