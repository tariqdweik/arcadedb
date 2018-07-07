/*
 * Copyright (c) 2018 - Arcade Analytics LTD (https://arcadeanalytics.com)
 */

package com.arcadedb.server.http.handler;

import com.arcadedb.exception.RecordNotFoundException;
import com.arcadedb.server.ha.HAServer;
import com.arcadedb.server.http.HttpServer;
import io.undertow.server.HttpServerExchange;

public class ServersHandler extends AbstractHandler {
  public ServersHandler(final HttpServer httpServer) {
    super(httpServer);
  }

  @Override
  public void execute(final HttpServerExchange exchange) {
    try {
      exchange.setStatusCode(200);

      final HAServer ha = httpServer.getServer().getHA();
      if (ha == null) {
        exchange.getResponseSender().send("{}");
      } else {
        final String masterServer = ha.isLeader() ? ha.getServer().getHttpServer().getListeningAddress() : ha.getLeader().getRemoteHTTPAddress();
        final String replicaServers = ha.getReplicaServersHTTPAddressesList();

        exchange.getResponseSender().send("{ \"masterServer\": \"" + masterServer + "\", \"replicaServers\" : \"" + replicaServers + "\"}");
      }

    } catch (RecordNotFoundException e) {
      exchange.setStatusCode(404);
      exchange.getResponseSender().send("{ \"error\" : \"Record id is null\"}");
    }
  }
}