package com.arcadedb.server;

import com.arcadedb.database.PDatabase;
import com.arcadedb.database.PDatabaseFactory;
import com.arcadedb.engine.PPaginatedFile;
import com.arcadedb.serializer.PJsonSerializer;
import io.undertow.Undertow;
import io.undertow.server.HttpHandler;
import io.undertow.server.RoutingHandler;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public class PHttpServer {
  private Undertow                         server;
  private ConcurrentMap<String, PDatabase> databases         = new ConcurrentHashMap<>();
  private PJsonSerializer                  jsonSerializer    = new PJsonSerializer();
  private String                           databaseDirectory = "/personal/Development/arcadedb/target/database/";

  public PHttpServer() {
    final HttpHandler routes = new RoutingHandler().get("/query/{database}/{text}", new PQueryHandler(this))
        .get("/record/{database}/{rid}", new PRecordHandler(this));
    server = Undertow.builder().addHttpListener(8080, "localhost").setHandler(routes).build();
  }

  public void close() {
    server.stop();
    for (PDatabase db : databases.values())
      db.close();
  }

  public PJsonSerializer getJsonSerializer() {
    return jsonSerializer;
  }

  public PDatabase getDatabase(final String databaseName) {
    PDatabase db = databases.get(databaseName);
    if (db == null) {
      db = new PDatabaseFactory(databaseDirectory + "/" + databaseName, PPaginatedFile.MODE.READ_WRITE).acquire();

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
    new PHttpServer().run();
  }
}
