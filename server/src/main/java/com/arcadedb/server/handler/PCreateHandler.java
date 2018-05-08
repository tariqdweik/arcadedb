/*
 * Copyright (c) 2018 - Arcade Analytics LTD (https://arcadeanalytics.com)
 */

package com.arcadedb.server.handler;

import com.arcadedb.database.Database;
import com.arcadedb.exception.RecordNotFoundException;
import com.arcadedb.server.HttpServer;
import io.undertow.server.HttpServerExchange;

import java.util.Deque;

public class PCreateHandler extends PBasicHandler {
  public PCreateHandler(final HttpServer httpServer) {
    super(httpServer);
  }

  @Override
  public void execute(final HttpServerExchange exchange, final Database database) {
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

    } catch (RecordNotFoundException e) {
      exchange.setStatusCode(404);
      exchange.getResponseSender().send("{ \"error\" : \"Record id is null\"}");
    }
  }
}