package com.arcadedb.server.handler;

import com.arcadedb.database.PDatabase;
import com.arcadedb.server.PHttpServer;
import com.arcadedb.utility.PLogManager;
import org.eclipse.jetty.server.Request;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;

public abstract class PBasicHandler {
  protected static final String CONTENT_TYPE_JSON = "application/json";

  protected final PHttpServer httpServer;

  public PBasicHandler(final PHttpServer pHttpServer) {
    this.httpServer = pHttpServer;
  }

  public abstract String getURL();

  protected abstract void execute(HttpServletRequest request, HttpServletResponse response, PDatabase database);

  public void handle(final String url, final Request r, final HttpServletRequest request, final HttpServletResponse response,
      final PDatabase database) {
    try {

      execute(request, response, database);

      response.setContentType(CONTENT_TYPE_JSON);

    } catch (Exception e) {
      try {
        response.sendError(HttpServletResponse.SC_INTERNAL_SERVER_ERROR,
            "{ \"error\" : \"Internal error\", \"detail\":\"" + e.toString() + "\"}");
      } catch (IOException e1) {
        PLogManager.instance().error(this, "Error on sending error back to the client", e);
      }
    }
  }
}
