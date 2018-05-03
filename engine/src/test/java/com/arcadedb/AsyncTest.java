package com.arcadedb;

import com.arcadedb.database.*;
import com.arcadedb.database.async.PErrorCallback;
import com.arcadedb.database.async.POkCallback;
import com.arcadedb.engine.PPaginatedFile;
import com.arcadedb.schema.PDocumentType;
import com.arcadedb.utility.PFileUtils;
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
    final PDatabase db = new PDatabaseFactory(DB_PATH, PPaginatedFile.MODE.READ_WRITE).acquire();
    db.drop();
  }

  @Test
  public void testScan() {
    final PDatabase db = new PDatabaseFactory(DB_PATH, PPaginatedFile.MODE.READ_ONLY).acquire();
    db.begin();
    try {
      final AtomicLong callbackInvoked = new AtomicLong();

      db.asynch().scanType(TYPE_NAME,true, new PDocumentCallback() {
        @Override
        public boolean onRecord(PDocument record) {
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
    final PDatabase db = new PDatabaseFactory(DB_PATH, PPaginatedFile.MODE.READ_ONLY).acquire();
    db.begin();
    try {
      final AtomicLong callbackInvoked = new AtomicLong();

      db.asynch().scanType(TYPE_NAME,true, new PDocumentCallback() {
        @Override
        public boolean onRecord(PDocument record) {
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
    PFileUtils.deleteRecursively(new File(DB_PATH));

    final PDatabase database = new PDatabaseFactory(DB_PATH, PPaginatedFile.MODE.READ_WRITE).acquire();
    database.begin();
    try {

      Assertions.assertFalse(database.getSchema().existsType(TYPE_NAME));

      final AtomicLong okCallbackInvoked = new AtomicLong();

      database.asynch().setCommitEvery(5000);
      database.asynch().setParallelLevel(3);
      database.asynch().onOk(new POkCallback() {
        @Override
        public void call() {
          okCallbackInvoked.incrementAndGet();
        }
      });

      database.asynch().onError(new PErrorCallback() {
        @Override
        public void call(Exception exception) {
          Assertions.fail("Error on creating async record", exception);
        }
      });

      final PDocumentType type = database.getSchema().createDocumentType(TYPE_NAME, 3);
      type.createProperty("id", Integer.class);
      type.createProperty("name", String.class);
      type.createProperty("surname", String.class);
      database.getSchema().createClassIndexes(TYPE_NAME, new String[] { "id" }, 20000);

      database.commit();

      for (int i = 0; i < total; ++i) {
        final PModifiableDocument v = database.newDocument(TYPE_NAME);
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