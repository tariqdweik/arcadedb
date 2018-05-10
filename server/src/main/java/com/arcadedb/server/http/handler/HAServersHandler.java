/*
 * Copyright (c) 2018 - Arcade Analytics LTD (https://arcadeanalytics.com)
 */

package com.arcadedb.server.http.handler;

import com.arcadedb.exception.RecordNotFoundException;
import com.arcadedb.server.http.HttpServer;
import io.undertow.server.HttpServerExchange;
import org.json.JSONObject;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class HAServersHandler extends AbstractHandler {
  public HAServersHandler(final HttpServer httpServer) {
    super(httpServer);
  }

  @Override
  public void execute(final HttpServerExchange exchange) {
    try {
      final String payload = parseRequestPayload(exchange);

      final JSONObject json = new JSONObject(payload);

      List<String> leavingServers = new ArrayList<>();
      List<String> joiningServers = new ArrayList<>();
      leavingServers.add("1");
      joiningServers.add("server.4=localhost:1234:1235;1236");

      final byte[] config = httpServer.getServer().getZKServer().reconfig(joiningServers, leavingServers);

      final String result = new String(config);

      System.out.println(result);

      exchange.setStatusCode(200);
      exchange.getResponseSender().send("{ \"result\" : \"" + result + "\"}");

    } catch (RecordNotFoundException e) {
      exchange.setStatusCode(404);
      exchange.getResponseSender().send("{ \"error\" : \"Record id is null\"}");
    } catch (IOException e) {
      e.printStackTrace();
      exchange.setStatusCode(500);
      exchange.getResponseSender().send("{ \"error\" : \"Error on executing create record\"}");
    }
  }
}