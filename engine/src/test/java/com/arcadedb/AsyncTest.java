/*
 * Copyright (c) 2018 - Arcade Analytics LTD (https://arcadeanalytics.com)
 */

package com.arcadedb;

import com.arcadedb.database.*;
import com.arcadedb.database.async.ErrorCallback;
import com.arcadedb.database.async.OkCallback;
import com.arcadedb.engine.PaginatedFile;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.utility.FileUtils;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.util.concurrent.atomic.AtomicLong;

public class AsyncTest {
  private static final int    TOT       = 10000;
  private static final String TYPE_NAME = "V";
  private static final String DB_PATH   = "target/database/testdb";

  @BeforeAll
  public static void populate() {
    populate(TOT);
  }

  @AfterAll
  public static void drop() {
    final Database db = new DatabaseFactory(DB_PATH, PaginatedFile.MODE.READ_WRITE).open();
    db.drop();
  }

  @Test
  public void testScan() {
    final Database db = new DatabaseFactory(DB_PATH, PaginatedFile.MODE.READ_ONLY).open();
    db.begin();
    try {
      final AtomicLong callbackInvoked = new AtomicLong();

      db.asynch().scanType(TYPE_NAME,true, new DocumentCallback() {
        @Override
        public boolean onRecord(Document record) {
          callbackInvoked.incrementAndGet();
          return true;
        }
      });

      Assertions.assertEquals(TOT, callbackInvoked.get());

    } finally {
      db.close();
    }
  }

  @Test
  public void testScanInterrupt() {
    final Database db = new DatabaseFactory(DB_PATH, PaginatedFile.MODE.READ_ONLY).open();
    db.begin();
    try {
      final AtomicLong callbackInvoked = new AtomicLong();

      db.asynch().scanType(TYPE_NAME,true, new DocumentCallback() {
        @Override
        public boolean onRecord(Document record) {
          if (callbackInvoked.get() > 9)
            return false;

          return callbackInvoked.getAndIncrement() < 10;
        }
      });

      Assertions.assertTrue(callbackInvoked.get() < 20);

    } finally {
      db.close();
    }
  }

  private static void populate(final int total) {
    FileUtils.deleteRecursively(new File(DB_PATH));

    final Database database = new DatabaseFactory(DB_PATH, PaginatedFile.MODE.READ_WRITE).create();
    database.begin();
    try {

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
      database.getSchema().createClassIndexes(true, TYPE_NAME, new String[] { "id" }, 20000);

      database.commit();

      for (int i = 0; i < total; ++i) {
        final ModifiableDocument v = database.newDocument(TYPE_NAME);
        v.set("id", i);
        v.set("name", "Jay");
        v.set("surname", "Miner");

        database.asynch().createRecord(v);

      }

      database.asynch().waitCompletion();

      Assertions.assertTrue(okCallbackInvoked.get()>0);
    } finally {
      database.close();
    }
  }
}