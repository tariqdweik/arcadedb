/*
 * Copyright (c) 2018 - Arcade Analytics LTD (https://arcadeanalytics.com)
 */

package com.arcadedb.mongodbw;

import com.arcadedb.ContextConfiguration;
import com.arcadedb.server.ArcadeDBServer;
import com.arcadedb.server.ServerPlugin;
import de.bwaldvogel.mongo.MongoDatabase;
import de.bwaldvogel.mongo.MongoServer;
import de.bwaldvogel.mongo.backend.AbstractMongoBackend;
import de.bwaldvogel.mongo.exception.MongoServerException;

public class MongoDBWrapperPlugin implements ServerPlugin {
  private MongoServer          mongoDBServer;
  private ArcadeDBServer       server;
  private ContextConfiguration configuration;

  @Override
  public void configure(final ArcadeDBServer arcadeDBServer, final ContextConfiguration configuration) {
    this.server = arcadeDBServer;
    this.configuration = configuration;
  }

  @Override
  public void startService() {
    mongoDBServer = new MongoServer(new AbstractMongoBackend() {
      @Override
      protected MongoDatabase openOrCreateDatabase(final String databaseName) throws MongoServerException {
        return new MongoDBDatabaseWrapper(server, databaseName, this);
      }

      @Override
      public void close() {
      }
    });
    mongoDBServer.bind("localhost", 27017);
  }

  @Override
  public void stopService() {
    mongoDBServer.shutdown();
  }
}