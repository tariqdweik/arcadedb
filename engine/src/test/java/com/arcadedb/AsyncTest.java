/*
 * Copyright (c) 2018 - Arcade Analytics LTD (https://arcadeanalytics.com)
 */

package com.arcadedb;

import com.arcadedb.database.Document;
import com.arcadedb.database.DocumentCallback;
import com.arcadedb.database.MutableDocument;
import com.arcadedb.database.async.ErrorCallback;
import com.arcadedb.database.async.OkCallback;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.SchemaImpl;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.concurrent.atomic.AtomicLong;

public class AsyncTest extends BaseTest {
  private static final int    TOT       = 10000;
  private static final String TYPE_NAME = "V";

  @Test
  public void testScan() {
    database.begin();
    try {
      final AtomicLong callbackInvoked = new AtomicLong();

      database.asynch().scanType(TYPE_NAME, true, new DocumentCallback() {
        @Override
        public boolean onRecord(Document record) {
          callbackInvoked.incrementAndGet();
          return true;
        }
      });

      Assertions.assertEquals(TOT, callbackInvoked.get());
    } finally {
      database.commit();
    }
  }

  @Test
  public void testScanInterrupt() {
    database.begin();
    try {
      final AtomicLong callbackInvoked = new AtomicLong();

      database.asynch().scanType(TYPE_NAME, true, new DocumentCallback() {
        @Override
        public boolean onRecord(Document record) {
          if (callbackInvoked.get() > 9)
            return false;

          return callbackInvoked.getAndIncrement() < 10;
        }
      });

      Assertions.assertTrue(callbackInvoked.get() < 20);

    } finally {
      database.commit();
    }
  }

  @Override
  protected void beginTest() {
    database.begin();

    Assertions.assertFalse(database.getSchema().existsType(TYPE_NAME));

    final AtomicLong okCallbackInvoked = new AtomicLong();

    database.asynch().setCommitEvery(5000);
    database.asynch().setParallelLevel(3);
    database.asynch().onOk(new OkCallback() {
      @Override
      public void call() {
        okCallbackInvoked.incrementAndGet();
      }
    });

    database.asynch().onError(new ErrorCallback() {
      @Override
      public void call(Exception exception) {
        Assertions.fail("Error on creating async record", exception);
      }
    });

    final DocumentType type = database.getSchema().createDocumentType(TYPE_NAME, 3);
    type.createProperty("id", Integer.class);
    type.createProperty("name", String.class);
    type.createProperty("surname", String.class);
    database.getSchema().createIndexes(SchemaImpl.INDEX_TYPE.LSM_TREE, true, TYPE_NAME, new String[] { "id" }, 20000);

    database.commit();

    for (int i = 0; i < TOT; ++i) {
      final MutableDocument v = database.newDocument(TYPE_NAME);
      v.set("id", i);
      v.set("name", "Jay");
      v.set("surname", "Miner");

      database.asynch().createRecord(v);

    }

    database.asynch().waitCompletion();

    Assertions.assertTrue(okCallbackInvoked.get() > 0);
  }
}