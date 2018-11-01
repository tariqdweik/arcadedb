/*
 * Copyright (c) 2018 - Arcade Analytics LTD (https://arcadeanalytics.com)
 */

package com.arcadedb.server.ha;

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.database.Database;
import com.arcadedb.database.RID;
import com.arcadedb.database.Record;
import com.arcadedb.exception.NeedRetryException;
import com.arcadedb.exception.RecordNotFoundException;
import com.arcadedb.exception.TransactionException;
import com.arcadedb.graph.MutableVertex;
import com.arcadedb.index.IndexCursor;
import com.arcadedb.index.RangeIndex;
import com.arcadedb.log.LogManager;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.server.BaseGraphServerTest;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.*;

public abstract class ReplicationServerTest extends BaseGraphServerTest {
  private static final int DEFAULT_MAX_RETRIES = 30;

  public ReplicationServerTest() {
    GlobalConfiguration.HA_REPLICATION_INCOMING_PORTS.setValue("2424-2500");
  }

  protected int getServerCount() {
    return 3;
  }

  protected int getTxs() {
    return 1000;
  }

  protected int getVerticesPerTx() {
    return 500;
  }

  @Test
  public void testReplication() {
    testReplication(0);
  }

  public void testReplication(final int serverId) {
    checkDatabases();

    Database db = getServerDatabase(serverId, getDatabaseName());
    db.begin();

    Assertions.assertEquals(1, db.countType(VERTEX1_TYPE_NAME, true), "TEST: Check for vertex count for server" + 0);

    LogManager.instance().info(this, "TEST: Executing %s transactions with %d vertices each...", getTxs(), getVerticesPerTx());

    final long total = getTxs() * getVerticesPerTx();
    long counter = 0;

    for (int tx = 0; tx < getTxs(); ++tx) {
      final long lastGoodCounter = counter;

      for (int retry = 0; retry < getMaxRetry(); ++retry) {
        try {
          db.begin();

          for (int i = 0; i < getVerticesPerTx(); ++i) {
            final MutableVertex v1 = db.newVertex(VERTEX1_TYPE_NAME);
            v1.set("id", ++counter);
            v1.set("name", "distributed-test");
            v1.save();
          }

          db.commit();
          break;

        } catch (TransactionException | NeedRetryException e) {
          LogManager.instance().info(this, "TEST: - RECEIVED ERROR: %s (RETRY %d/%d)", e.toString(), retry, getMaxRetry());
          if (retry >= getMaxRetry() - 1)
            throw e;
          counter = lastGoodCounter;
        }
      }

      if (counter % (total / 10) == 0) {
        LogManager.instance().info(this, "TEST: - Progress %d/%d", counter, (getTxs() * getVerticesPerTx()));
        if (isPrintingConfigurationAtEveryStep())
          getLeaderServer().getHA().printClusterConfiguration();
      }

      db.begin();
    }

    LogManager.instance().info(this, "Done");

    Assertions.assertEquals(1 + getTxs() * getVerticesPerTx(), db.countType(VERTEX1_TYPE_NAME, true), "Check for vertex count for server" + 0);

    try {
      Thread.sleep(1000);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }

    // CHECK INDEXES ARE REPLICATED CORRECTLY
    for (int s : getServerToCheck()) {
      checkEntriesOnServer(s);
    }

    onAfterTest();
  }

  protected int getMaxRetry() {
    return DEFAULT_MAX_RETRIES;
  }

  protected void checkDatabases() {
    for (int s = 0; s < getServerCount(); ++s) {
      Database db = getServerDatabase(s, getDatabaseName());
      db.begin();
      try {
        Assertions.assertEquals(1, db.countType(VERTEX1_TYPE_NAME, true), "Check for vertex count for server" + s);
        Assertions.assertEquals(2, db.countType(VERTEX2_TYPE_NAME, true), "Check for vertex count for server" + s);

        Assertions.assertEquals(1, db.countType(EDGE1_TYPE_NAME, true), "Check for edge count for server" + s);
        Assertions.assertEquals(2, db.countType(EDGE2_TYPE_NAME, true), "Check for edge count for server" + s);

      } catch (Exception e) {
        e.printStackTrace();
        Assertions.fail("Error on checking on server" + s);
      }
    }
  }

  protected void onAfterTest() {
  }

  protected boolean isPrintingConfigurationAtEveryStep() {
    return false;
  }

  protected void checkEntriesOnServer(final int s) {
    final Database db = getServerDatabase(s, getDatabaseName());
    db.begin();
    try {
      final long recordInDb = db.countType(VERTEX1_TYPE_NAME, true);
      Assertions.assertTrue(recordInDb <= 1 + getTxs() * getVerticesPerTx(), "TEST: Check for vertex count for server" + s);

      final List<DocumentType.IndexMetadata> indexes = db.getSchema().getType(VERTEX1_TYPE_NAME).getIndexMetadataByProperties("id");
      long total = 0;
      for (int i = 0; i < indexes.size(); ++i) {
        for (IndexCursor it = ((RangeIndex) indexes.get(i).index).iterator(true); it.hasNext(); ) {
          it.next();
          ++total;
        }
      }

      LogManager.instance().info(this, "TEST: Entries in the index (%d) > records in database (%d)", total, recordInDb);

      final Map<RID, Set<String>> ridsFoundInIndex = new HashMap<>();
      long total2 = 0;
      long missingsCount = 0;
      for (int i = 0; i < indexes.size(); ++i) {
        final RangeIndex idx = ((RangeIndex) indexes.get(i).index);

        for (IndexCursor it = idx.iterator(true); it.hasNext(); ) {
          final RID rid = it.next();
          ++total2;

          Set<String> rids = ridsFoundInIndex.get(rid);
          if (rids == null) {
            rids = new HashSet<>(indexes.size());
            ridsFoundInIndex.put(rid, rids);
          }

          rids.add(idx.getName());

          Record record = null;
          try {
            record = rid.getRecord(true);
          } catch (RecordNotFoundException e) {
            // IGNORE IT, CAUGHT BELOW
          }

          if (record == null) {
            LogManager.instance().info(this, "TEST: - Cannot find record %s in database even if it's present in the index (null)", rid);
            missingsCount++;
          }

        }
      }

      Assertions.assertEquals(recordInDb, ridsFoundInIndex.size(), "TEST: Found " + ridsFoundInIndex + " missing records");
      Assertions.assertEquals(0, missingsCount);
      Assertions.assertEquals(total, total2);

    } catch (Exception e) {
      e.printStackTrace();
      Assertions.fail("TEST: Error on checking on server" + s);
    }
  }
}