/*
 * Copyright (c) 2018 - Arcade Analytics LTD (https://arcadeanalytics.com)
 */

package com.arcadedb.server.http.handler;

import com.arcadedb.database.Database;
import com.arcadedb.database.MutableDocument;
import com.arcadedb.server.http.HttpServer;
import io.undertow.server.HttpServerExchange;
import org.json.JSONObject;

import java.io.IOException;

public class CreateDocumentHandler extends DatabaseAbstractHandler {
  public CreateDocumentHandler(final HttpServer httpServer) {
    super(httpServer);
  }

  @Override
  public void execute(final HttpServerExchange exchange, final Database database) throws IOException {
    final String payload = parseRequestPayload(exchange);

    final JSONObject json = new JSONObject(payload);

    final String type = json.getString("@type");
    if (type == null) {
      exchange.setStatusCode(404);
      exchange.getResponseSender().send("{ \"error\" : \"@type attribute not found in the record payload\"}");
      return;
    }

    httpServer.getServer().getServerMetrics().meter("http.create-record").mark();

    final MutableDocument document = database.newDocument(type);
    document.save();

    exchange.setStatusCode(200);
    exchange.getResponseSender().send("{ \"result\" : \"" + document.getIdentity() + "\"}");
  }
}