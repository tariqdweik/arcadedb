/*
 * Copyright (c) - Arcade Data LTD (https://arcadedata.com)
 */

package com.arcadedb.mongodbw;

import com.arcadedb.database.Database;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.server.ArcadeDBServer;
import de.bwaldvogel.mongo.MongoBackend;
import de.bwaldvogel.mongo.MongoCollection;
import de.bwaldvogel.mongo.MongoDatabase;
import de.bwaldvogel.mongo.backend.LimitedList;
import de.bwaldvogel.mongo.backend.Utils;
import de.bwaldvogel.mongo.bson.Document;
import de.bwaldvogel.mongo.exception.MongoServerError;
import de.bwaldvogel.mongo.exception.MongoServerException;
import de.bwaldvogel.mongo.wire.message.MongoDelete;
import de.bwaldvogel.mongo.wire.message.MongoInsert;
import de.bwaldvogel.mongo.wire.message.MongoQuery;
import de.bwaldvogel.mongo.wire.message.MongoUpdate;
import io.netty.channel.Channel;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.Level;

public class MongoDBDatabaseWrapper implements MongoDatabase {
  protected final ArcadeDBServer                     server;
  protected final Database                           database;
  protected final MongoBackend                       backend;
  protected final Map<String, MongoCollection<Long>> collections = new ConcurrentHashMap();
  protected final Map<Channel, List<Document>>       lastResults = new ConcurrentHashMap();

  public MongoDBDatabaseWrapper(final ArcadeDBServer server, final String databaseName, final MongoBackend backend) {
    this.server = server;
    this.database = server.getDatabase(databaseName);
    this.backend = backend;

    for (DocumentType dt : database.getSchema().getTypes()) {
      collections.put(dt.getName(), new MongoDBCollectionWrapper(server, database, dt.getName()));
    }
  }

  @Override
  public String getDatabaseName() {
    return database.getName();
  }

  @Override
  public void handleClose(Channel channel) {
    database.close();
  }

  @Override
  public Document handleCommand(Channel channel, String command, Document document) throws MongoServerException {
    try {
      if (command.equalsIgnoreCase("create"))
        return createCollection(document);
      else if (command.equalsIgnoreCase("count"))
        return countCollection(document);
      else if (command.equalsIgnoreCase("insert"))
        return insertDocument(channel, document);
      else {
        server.log(this, Level.SEVERE, "Received unsupported command from MongoDB client '%s', (document=%s)", command, document);
        throw new UnsupportedOperationException(
            String.format("Received unsupported command from MongoDB client '%s', (document=%s)", command, document));
      }
    } catch (Exception e) {
      throw new MongoServerException("Error on executing MongoDB '" + command + "' command", e);
    }
  }

  @Override
  public Iterable<Document> handleQuery(final MongoQuery query) throws MongoServerException {
    try {
      this.clearLastStatus(query.getChannel());
      final String collectionName = query.getCollectionName();
      final MongoCollection<Long> collection = collections.get(collectionName);
      if (collection == null) {
        return Collections.emptyList();
      } else {
        int numSkip = query.getNumberToSkip();
        int numReturn = query.getNumberToReturn();
        Document fieldSelector = query.getReturnFieldSelector();
        return collection.handleQuery(query.getQuery(), numSkip, numReturn, fieldSelector);
      }
    } catch (Exception e) {
      throw new MongoServerException("Error on executing MongoDB query", e);
    }
  }

  @Override
  public void handleInsert(final MongoInsert mongoInsert) {

  }

  @Override
  public void handleDelete(final MongoDelete mongoDelete) {

  }

  @Override
  public void handleUpdate(final MongoUpdate mongoUpdate) {

  }

  @Override
  public boolean isEmpty() {
    return false;
  }

  @Override
  public MongoCollection<?> resolveCollection(final String collectionName, final boolean throwExceptionIfNotFound) {
    return null;
  }

  @Override
  public void drop() {
    database.drop();
  }

  @Override
  public void dropCollection(final String collectionName) {
    database.getSchema().dropType(collectionName);
  }

  @Override
  public void moveCollection(MongoDatabase mongoDatabase, MongoCollection<?> mongoCollection, String s) {
    throw new UnsupportedOperationException();
  }

  @Override
  public MongoCollection<?> unregisterCollection(final String collectionName) {
    return null;
  }

  private Document createCollection(final Document document) {
    database.getSchema().createDocumentType((String) document.get("create"), 1);
    return responseOk();
  }

  private Document countCollection(final Document document) throws MongoServerException {
    final String collectionName = document.get("count").toString();
    database.countType(collectionName, false);

    final Document response = responseOk();

    final MongoCollection<Long> collection = collections.get(collectionName);

    if (collection == null) {
      response.put("missing", Boolean.TRUE);
      response.put("n", 0);
    } else {
      Document queryObject = (Document) document.get("query");
      int limit = this.getOptionalNumber(document, "limit", -1);
      int skip = this.getOptionalNumber(document, "skip", 0);
      response.put("n", collection.count(queryObject, skip, limit));
    }

    return response;
  }

  private Document insertDocument(final Channel channel, final Document query) throws MongoServerException {
    String collectionName = query.get("insert").toString();
    boolean isOrdered = Utils.isTrue(query.get("ordered"));
    List<Document> documents = (List) query.get("documents");
    List<Document> writeErrors = new ArrayList();

    int n = 0;
    try {
      this.clearLastStatus(channel);

      try {
        if (collectionName.startsWith("system.")) {
          throw new MongoServerError(16459, "attempt to insert in system namespace");
        } else {
          MongoCollection<Long> collection = getOrCreateCollection(collectionName);
          n = collection.insertDocuments(documents);

          assert n == documents.size();

          Document result = new Document("n", n);
          this.putLastResult(channel, result);
        }
      } catch (MongoServerError var7) {
        this.putLastError(channel, var7);
        throw var7;
      }

      ++n;
    } catch (MongoServerError e) {
      Document error = new Document();
      error.put("index", n);
      error.put("errmsg", e.getMessage());
      error.put("code", e.getCode());
      error.putIfNotNull("codeName", e.getCodeName());
      writeErrors.add(error);
    }

    Document result = new Document();
    result.put("n", n);
    if (!writeErrors.isEmpty()) {
      result.put("writeErrors", writeErrors);
    }

    Utils.markOkay(result);
    return result;
  }

  private MongoCollection<Long> getOrCreateCollection(final String collectionName) {
    MongoCollection<Long> collection = collections.get(collectionName);
    if (collection == null) {
      collection = new MongoDBCollectionWrapper(server, database, collectionName);
      collections.put(collectionName, collection);
    }
    return collection;
  }

  private Document responseOk() {
    Document response = new Document();
    Utils.markOkay(response);
    return response;
  }

  private int getOptionalNumber(final Document query, final String fieldName, final int defaultValue) {
    final Number limitNumber = (Number) query.get(fieldName);
    return limitNumber != null ? limitNumber.intValue() : defaultValue;
  }

  private synchronized void clearLastStatus(final Channel channel) {
    List<Document> results = this.lastResults.get(channel);
    if (results == null) {
      results = new LimitedList(10);
      this.lastResults.put(channel, results);
    }
    results.add(null);
  }

  private synchronized void putLastResult(final Channel channel, final Document result) {
    final List<Document> results = this.lastResults.get(channel);
    final Document last = results.get(results.size() - 1);
    if (last != null)
      throw new IllegalStateException("last result already set: " + last);
    results.set(results.size() - 1, result);
  }

  private void putLastError(final Channel channel, final MongoServerException ex) {
    final Document error = new Document();
    if (ex instanceof MongoServerError) {
      final MongoServerError err = (MongoServerError) ex;
      error.put("err", err.getMessage());
      error.put("code", err.getCode());
      error.putIfNotNull("codeName", err.getCodeName());
    } else {
      error.put("err", ex.getMessage());
    }

    error.put("connectionId", channel.id().asShortText());
    this.putLastResult(channel, error);
  }
}