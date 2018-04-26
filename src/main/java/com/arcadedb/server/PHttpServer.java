package com.arcadedb.server;

import com.arcadedb.database.PDatabase;
import com.arcadedb.database.PDatabaseFactory;
import com.arcadedb.engine.PPaginatedFile;
import com.arcadedb.serializer.PJsonSerializer;
import com.arcadedb.server.handler.PQueryHandler;
import com.arcadedb.server.handler.PRecordHandler;
import io.undertow.Undertow;
import io.undertow.server.HttpHandler;
import io.undertow.server.RoutingHandler;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public class PHttpServer {
  private       Undertow                         server;
  private       ConcurrentMap<String, PDatabase> databases      = new ConcurrentHashMap<>();
  private       PJsonSerializer                  jsonSerializer = new PJsonSerializer();
  private final PHttpServerConfiguration         configuration;

  public PHttpServer(final PHttpServerConfiguration configuration) {
    this.configuration = configuration;

    final HttpHandler routes = new RoutingHandler().get("/query/{database}/{text}", new PQueryHandler(this))
        .get("/record/{database}/{rid}", new PRecordHandler(this));
    server = Undertow.builder().addHttpListener(configuration.bindPort, configuration.bindServer).setHandler(routes).build();
  }

  public void close() {
    server.stop();
    for (PDatabase db : databases.values())
      db.close();
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

  private void run() {
    server.start();
  }

  public static void main(final String[] args) {
    new PHttpServer(PHttpServerConfiguration.create()).run();
  }
}
