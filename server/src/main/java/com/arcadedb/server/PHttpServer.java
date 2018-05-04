package com.arcadedb.server;

import com.arcadedb.PConstants;
import com.arcadedb.database.PDatabase;
import com.arcadedb.database.PDatabaseFactory;
import com.arcadedb.engine.PPaginatedFile;
import com.arcadedb.serializer.PJsonSerializer;
import com.arcadedb.server.handler.PBasicHandler;
import com.arcadedb.server.handler.PGetRecordHandler;
import com.arcadedb.server.handler.PPostCommandHandler;
import com.arcadedb.utility.PLogManager;
import org.eclipse.jetty.server.HttpConnection;
import org.eclipse.jetty.server.Request;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.ServerConnector;
import org.eclipse.jetty.server.handler.AbstractHandler;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public class PHttpServer {
  private final PHttpServerConfiguration         configuration;
  private       Server                           server;
  private       ConcurrentMap<String, PDatabase> databases      = new ConcurrentHashMap<>();
  private       PJsonSerializer                  jsonSerializer = new PJsonSerializer();

  private Map<String, PBasicHandler> handlers = new HashMap<>();
  private PPostCommandHandler        command  = new PPostCommandHandler(this);
  private PGetRecordHandler          record   = new PGetRecordHandler(this);

  public PHttpServer(final PHttpServerConfiguration configuration) {
    this.configuration = configuration;

    server = new Server();

    // HTTP connector
    final ServerConnector http = new ServerConnector(server);
    http.setHost(configuration.bindServer);
    http.setPort(configuration.bindPort);
    //http.setIdleTimeout(30000);

    server.addConnector(http);

    handlers.put(command.getURL(), command);
    handlers.put(record.getURL(), record);

    server.setHandler(new AbstractHandler() {
      @Override
      public void handle(String s, Request req, HttpServletRequest request, HttpServletResponse response)
          throws IOException, ServletException {

        final int secondSlashPos = s.indexOf('/', 1);
        if (secondSlashPos == -1) {
          response.sendError(400, "Not supported");
          return;
        }

        final String databaseName;

        final int thirdSlashPos = s.indexOf('/', secondSlashPos + 1);
        if (thirdSlashPos == -1)
          databaseName = s.substring(secondSlashPos + 1);
        else
          databaseName = s.substring(secondSlashPos + 1, thirdSlashPos);

        if (databaseName.isEmpty()) {
          response.sendError(400, "Missing database name");
          return;
        }

        final String handler = s.substring(0, secondSlashPos);

        final PBasicHandler handlerImpl = handlers.get(handler);

        if (handlerImpl == null) {
          response.sendError(400, handler + " not supported");
          return;
        }

        final PDatabase database = getDatabase(databaseName);
        if (database == null) {
          response.sendError(400, "Database " + databaseName + " not found");
          return;
        }

        handlerImpl.handle(s, req, request, response, database);

        final Request base_request = (request instanceof Request) ?
            (Request) request :
            HttpConnection.getCurrentConnection().getHttpChannel().getRequest();
        base_request.setHandled(true);
      }
    });
  }

  public static void main(final String[] args) {
    new PHttpServer(PHttpServerConfiguration.create()).start();
  }

  public void start() {
    PLogManager.instance().info(this, "Starting " + PConstants.PRODUCT + " Server (server=%s port=%d)...", configuration.bindServer,
        configuration.bindPort);
    try {
      server.start();
    } catch (Exception e) {
      PLogManager.instance()
          .error(this, "Error on starting " + PConstants.PRODUCT + " Server (server=%s port=%d)...", e, configuration.bindServer,
              configuration.bindPort);
      throw new PServerException("Error on starting ArcadeDB Server", e);
    }
  }

  public void close() {
    PLogManager.instance().info(this, "Shutting down server...");
    try {
      server.stop();
      server.join();
      for (PDatabase db : databases.values())
        db.close();
      PLogManager.instance().info(this, "The server is down");
    } catch (Exception e) {
      PLogManager.instance()
          .error(this, "Error on stopping " + PConstants.PRODUCT + " Server (server=%s port=%d)...", e, configuration.bindServer,
              configuration.bindPort);
      throw new PServerException("Error on stopping ArcadeDB Server", e);
    }
  }

  public PJsonSerializer getJsonSerializer() {
    return jsonSerializer;
  }

  public synchronized PDatabase getDatabase(final String databaseName) {
    PDatabase db = databases.get(databaseName);
    if (db == null) {
      db = new PDatabaseFactory(configuration.databaseDirectory + "/" + databaseName, PPaginatedFile.MODE.READ_WRITE).acquire();

      final PDatabase oldDb = databases.putIfAbsent(databaseName, db);

      if (oldDb != null)
        db = oldDb;
    }

    return db;
  }
}
