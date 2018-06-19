/*
 * Copyright (c) 2018 - Arcade Analytics LTD (https://arcadeanalytics.com)
 */

package com.arcadedb.mongodbw;

import com.arcadedb.GlobalConfiguration;
import com.mongodb.MongoClient;
import com.mongodb.ServerAddress;
import org.bson.BsonDocument;
import org.bson.Document;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

public class MongoDBWTest extends BaseGraphServerTest {

  private static final int                                                   DEF_PORT = 27017;
  private              com.mongodb.client.MongoCollection<org.bson.Document> collection;
  private              MongoClient                                           client;

  @BeforeEach
  @Override
  public void startTest() {
    GlobalConfiguration.SERVER_PLUGINS.setValue("MongoDB:com.arcadedb.mongodbw.MongoDBWrapperPlugin");
    super.startTest();
  }

  @AfterEach
  @Override
  public void endTest() {
    GlobalConfiguration.SERVER_PLUGINS.setValue("");
    client.close();
    super.endTest();
  }

  @Test
  public void testSimpleInsertQuery() throws Exception {
    client = new MongoClient(new ServerAddress("localhost", DEF_PORT));
    client.getDatabase(getDatabaseName()).createCollection("MongoDBCollection");

    collection = client.getDatabase(getDatabaseName()).getCollection("MongoDBCollection");
    assertEquals(0, collection.count());

    Document obj = new Document("id", 0).append("name", "Jay").append("lastName", "Miner");
    collection.insertOne(obj);

    assertEquals(1, collection.count());

    assertEquals(obj, collection.find().first());

    assertEquals(obj, collection.find(BsonDocument.parse("{ name: \"Jay\" } ")).first());

    assertNull(collection.find(BsonDocument.parse("{ name: \"Jay2\" } ")).first());

    assertEquals(obj, collection.find(BsonDocument.parse("{ name: { $eq: \"Jay\" } } ")).first());

    assertEquals(obj, collection.find(BsonDocument.parse("{ name: { $ne: \"Jay2\" } } ")).first());

    assertEquals(obj, collection.find(BsonDocument.parse("{ name: { $in: [ \"Jay\", \"John\" ] } } ")).first());

    assertEquals(obj, collection.find(BsonDocument.parse("{ name: { $nin: [ \"Jay2\", \"John\" ] } } ")).first());

    assertEquals(obj, collection.find(BsonDocument.parse("{ name: { $lt: \"Jay2\" } } ")).first());

    assertEquals(obj, collection.find(BsonDocument.parse("{ name: { $lte: \"Jay2\" } } ")).first());

    assertEquals(obj, collection.find(BsonDocument.parse("{ name: { $gt: \"A\" } } ")).first());

    assertEquals(obj, collection.find(BsonDocument.parse("{ name: { $gte: \"A\" } } ")).first());
  }

  private boolean checkAreEquals(final Document first, final Document second) {
    if (first.size() != second.size())
      return false;

    for (Map.Entry<String, Object> entry : first.entrySet()) {
      final Object firstValue = entry.getValue();
      final Object secondValue = second.get(entry.getKey());

      if (firstValue == null) {
        if (secondValue != null)
          return false;
      } else if (!firstValue.equals(secondValue))
        return false;
    }

    return true;
  }
}