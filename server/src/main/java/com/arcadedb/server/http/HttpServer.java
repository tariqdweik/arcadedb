/*
 * Copyright (c) 2018 - Arcade Analytics LTD (https://arcadeanalytics.com)
 */

package com.arcadedb.server.http;

import com.arcadedb.ContextConfiguration;
import com.arcadedb.GlobalConfiguration;
import com.arcadedb.serializer.JsonSerializer;
import com.arcadedb.server.ArcadeDBServer;
import com.arcadedb.server.ServerException;
import com.arcadedb.server.http.handler.CommandHandler;
import com.arcadedb.server.http.handler.CreateDocumentHandler;
import com.arcadedb.server.http.handler.GetDocumentHandler;
import com.arcadedb.server.http.handler.HAServersHandler;
import com.arcadedb.utility.LogManager;
import io.undertow.Undertow;
import io.undertow.server.HttpHandler;
import io.undertow.server.RoutingHandler;

import java.net.BindException;

public class HttpServer {
  private       Undertow       server;
  private       JsonSerializer jsonSerializer = new JsonSerializer();
  private final ArcadeDBServer mainServer;

  public HttpServer(final ArcadeDBServer mainServer) {
    this.mainServer = mainServer;
  }

  public void stop() {
    if (server != null)
      server.stop();
  }

  public JsonSerializer getJsonSerializer() {
    return jsonSerializer;
  }

  public void start() {
    final ContextConfiguration configuration = mainServer.getConfiguration();

    final String host = configuration.getValueAsString(GlobalConfiguration.SERVER_HTTP_INCOMING_HOST);
    final boolean httpAutoIncrementPort = configuration.getValueAsBoolean(GlobalConfiguration.SERVER_HTTP_AUTOINCREMENT_PORT);
    int port = configuration.getValueAsInteger(GlobalConfiguration.SERVER_HTTP_INCOMING_PORT);

    LogManager.instance().info(this, "- Starting HTTP Server (host=%s port=%d)...", host, port);

    final HttpHandler routes = new RoutingHandler().post("/command/{database}/{command}", new CommandHandler(this))
        .get("/document/{database}/{rid}", new GetDocumentHandler(this))
        .post("/document/{database}", new CreateDocumentHandler(this)).post("/server", new HAServersHandler(this));

    do {
      try {
        server = Undertow.builder().addHttpListener(port, host).setHandler(routes).build();
        server.start();

        LogManager.instance().info(this, "- HTTP Server started (host=%s port=%d)", host, port);
        break;

      } catch (Exception e) {
        server = null;

        if (e.getCause() instanceof BindException) {
          // RETRY
          LogManager.instance().warn(this, "- HTTP Port %s not available", port);
          ++port;
          continue;
        }

        throw new ServerException("Error on starting HTTP Server", e);
      }
    } while (httpAutoIncrementPort);
  }

  public ArcadeDBServer getServer() {
    return mainServer;
  }
}
