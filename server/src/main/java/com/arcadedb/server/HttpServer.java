package com.arcadedb.server;

import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.engine.PaginatedFile;
import com.arcadedb.serializer.JsonSerializer;
import com.arcadedb.server.handler.PCommandHandler;
import com.arcadedb.server.handler.PRecordHandler;
import com.arcadedb.utility.LogManager;
import io.undertow.Undertow;
import io.undertow.server.HttpHandler;
import io.undertow.server.RoutingHandler;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public class HttpServer {
  private final HttpServerConfiguration         configuration;
  private       Undertow                        server;
  private       ConcurrentMap<String, Database> databases      = new ConcurrentHashMap<>();
  private       JsonSerializer                  jsonSerializer = new JsonSerializer();

  public HttpServer(final HttpServerConfiguration configuration) {
    this.configuration = configuration;

    final HttpHandler routes = new RoutingHandler().post("/command/{database}/{command}", new PCommandHandler(this))
        .get("/record/{database}/{rid}", new PRecordHandler(this));
    server = Undertow.builder().addHttpListener(configuration.bindPort, configuration.bindServer).setHandler(routes).build();
  }

  public static void main(final String[] args) {
    new HttpServer(HttpServerConfiguration.create()).start();
  }

  public void close() {
    LogManager.instance().info(this, "Shutting down server...");
    server.stop();
    for (Database db : databases.values())
      db.close();
    LogManager.instance().info(this, "The server is down");
  }

  public JsonSerializer getJsonSerializer() {
    return jsonSerializer;
  }

  public synchronized Database getDatabase(final String databaseName) {
    Database db = databases.get(databaseName);
    if (db == null) {
      db = new DatabaseFactory(configuration.databaseDirectory + "/" + databaseName, PaginatedFile.MODE.READ_WRITE)
          .setAutoTransaction(true).acquire();

      final Database oldDb = databases.putIfAbsent(databaseName, db);

      if (oldDb != null)
        db = oldDb;
    }

    return db;
  }

  public void start() {
    LogManager.instance()
        .info(this, "Starting ArcadeDB server (server=%s port=%d)...", configuration.bindServer, configuration.bindPort);
    server.start();
  }
}
