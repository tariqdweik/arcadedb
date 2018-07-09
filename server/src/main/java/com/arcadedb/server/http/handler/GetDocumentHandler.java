/*
 * Copyright (c) 2018 - Arcade Analytics LTD (https://arcadeanalytics.com)
 */

package com.arcadedb.server.http.handler;

import com.arcadedb.database.Database;
import com.arcadedb.database.Document;
import com.arcadedb.database.RID;
import com.arcadedb.server.http.HttpServer;
import io.undertow.server.HttpServerExchange;

import java.util.Deque;

public class GetDocumentHandler extends DatabaseAbstractHandler {
  public GetDocumentHandler(final HttpServer httpServer) {
    super(httpServer);
  }

  @Override
  public void execute(final HttpServerExchange exchange, final Database database) {
    final Deque<String> rid = exchange.getQueryParameters().get("rid");
    if (rid == null || rid.isEmpty()) {
      exchange.setStatusCode(400);
      exchange.getResponseSender().send("{ \"error\" : \"Record id is null\"}");
      return;
    }

    final String[] ridParts = rid.getFirst().split(":");

    final Document record = (Document) database.lookupByRID(new RID(database, Integer.parseInt(ridParts[0]), Long.parseLong(ridParts[1])), true);

    exchange.setStatusCode(200);
    exchange.getResponseSender().send("{ \"result\" : " + httpServer.getJsonSerializer().serializeRecord(record).toString() + "}");
  }
}