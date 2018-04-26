package com.arcadedb.server;

import io.undertow.server.HttpHandler;

public abstract class PBasicHandler implements HttpHandler {
  protected final PHttpServer httpServer;

  public PBasicHandler(final PHttpServer pHttpServer) {
    this.httpServer = pHttpServer;
  }
}
