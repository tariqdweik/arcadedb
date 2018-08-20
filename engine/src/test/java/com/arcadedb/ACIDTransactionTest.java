/*
 * Copyright (c) 2018 - Arcade Analytics LTD (https://arcadeanalytics.com)
 */

package com.arcadedb;

import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.MutableDocument;
import com.arcadedb.database.async.ErrorCallback;
import com.arcadedb.engine.WALException;
import com.arcadedb.engine.WALFile;
import com.arcadedb.exception.TransactionException;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.utility.LogManager;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

public class ACIDTransactionTest extends BaseTest {
  @Override
  protected void beginTest() {
    database.transaction(new Database.Transaction() {
      @Override
      public void execute(Database database) {
        if (!database.getSchema().existsType("V")) {
          final DocumentType v = database.getSchema().createDocumentType("V");

          v.createProperty("id", Integer.class);
          v.createProperty("name", String.class);
          v.createProperty("surname", String.class);
        }
      }
    });
  }

  @Test
  public void testAsyncTX() {
    final Database db = database;

    db.asynch().setTransactionSync(WALFile.FLUSH_TYPE.YES_NOMETADATA);
    db.asynch().setTransactionUseWAL(true);
    db.asynch().setCommitEvery(1);

    final int TOT = 1000;

    final AtomicInteger total = new AtomicInteger(0);

    try {
      for (; total.get() < TOT; total.incrementAndGet()) {
        final MutableDocument v = db.newDocument("V");
        v.set("id", total.get());
        v.set("name", "Crash");
        v.set("surname", "Test");

        db.asynch().createRecord(v);
      }

      db.asynch().waitCompletion();

      try {
        Thread.sleep(500);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        // IGNORE IT
      }

    } catch (TransactionException e) {
      Assertions.assertTrue(e.getCause() instanceof IOException);
    }

    ((DatabaseInternal) db).kill();

    verifyWALFilesAreStillPresent();

    verifyDatabaseWasNotClosedProperly();
    Assertions.assertEquals(TOT, database.countType("V", true));
  }

  @Test
  public void testCrashDuringTx() {
    final Database db = database;
    db.begin();
    try {
      final MutableDocument v = db.newDocument("V");
      v.set("id", 0);
      v.set("name", "Crash");
      v.set("surname", "Test");
      v.save();

    } finally {
      ((DatabaseInternal) db).kill();
    }

    verifyDatabaseWasNotClosedProperly();
    Assertions.assertEquals(0, database.countType("V", true));
  }

  @Test
  public void testIOExceptionAfterWALIsWritten() {
    final Database db = database;
    db.begin();

    try {
      final MutableDocument v = db.newDocument("V");
      v.set("id", 0);
      v.set("name", "Crash");
      v.set("surname", "Test");
      v.save();

      ((DatabaseInternal) db).registerCallback(DatabaseInternal.CALLBACK_EVENT.TX_AFTER_WAL_WRITE, new Callable<Void>() {
        @Override
        public Void call() throws IOException {
          throw new IOException("Test IO Exception");
        }
      });

      db.commit();

      Assertions.fail("Expected commit to fail");

    } catch (TransactionException e) {
      Assertions.assertTrue(e.getCause() instanceof WALException);
    }
    ((DatabaseInternal) db).kill();

    verifyWALFilesAreStillPresent();

    verifyDatabaseWasNotClosedProperly();
    Assertions.assertEquals(1, database.countType("V", true));
  }

  @Test
  public void testAsyncIOExceptionAfterWALIsWrittenLastRecords() {
    final Database db = database;

    final AtomicInteger errors = new AtomicInteger(0);

    db.asynch().setTransactionSync(WALFile.FLUSH_TYPE.YES_NOMETADATA);
    db.asynch().setTransactionUseWAL(true);
    db.asynch().setCommitEvery(1);
    db.asynch().onError(new ErrorCallback() {
      @Override
      public void call(Exception exception) {
        errors.incrementAndGet();
      }
    });

    final int TOT = 1000;

    final AtomicInteger total = new AtomicInteger(0);
    final AtomicInteger commits = new AtomicInteger(0);

    try {
      ((DatabaseInternal) db).registerCallback(DatabaseInternal.CALLBACK_EVENT.TX_AFTER_WAL_WRITE, new Callable<Void>() {
        @Override
        public Void call() throws IOException {
          if (commits.incrementAndGet() > TOT - 1) {
            LogManager.instance().info(this, "TEST: Causing IOException at commit %d...", commits.get());
            throw new IOException("Test IO Exception");
          }
          return null;
        }
      });

      for (; total.get() < TOT; total.incrementAndGet()) {
        final MutableDocument v = db.newDocument("V");
        v.set("id", 0);
        v.set("name", "Crash");
        v.set("surname", "Test");

        db.asynch().createRecord(v);
      }

      db.asynch().waitCompletion();

      try {
        Thread.sleep(500);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        // IGNORE IT
      }

      Assertions.assertEquals(1, errors.get());

    } catch (TransactionException e) {
      Assertions.assertTrue(e.getCause() instanceof IOException);
    }
    ((DatabaseInternal) db).kill();

    verifyWALFilesAreStillPresent();

    verifyDatabaseWasNotClosedProperly();
    Assertions.assertEquals(TOT, database.countType("V", true));
  }

  @Test
  public void testAsyncIOExceptionAfterWALIsWrittenManyRecords() {
    final Database db = database;

    final int TOT = 100000;

    final AtomicInteger total = new AtomicInteger(0);

    final AtomicInteger errors = new AtomicInteger(0);

    db.asynch().setTransactionSync(WALFile.FLUSH_TYPE.YES_NOMETADATA);
    db.asynch().setTransactionUseWAL(true);
    db.asynch().setCommitEvery(1000000);
    db.asynch().onError(new ErrorCallback() {
      @Override
      public void call(Exception exception) {
        errors.incrementAndGet();
      }
    });

    try {
      ((DatabaseInternal) db).registerCallback(DatabaseInternal.CALLBACK_EVENT.TX_AFTER_WAL_WRITE, new Callable<Void>() {
        @Override
        public Void call() throws IOException {
          if (total.incrementAndGet() > TOT - 10)
            throw new IOException("Test IO Exception");
          return null;
        }
      });

      for (; total.get() < TOT; total.incrementAndGet()) {
        final MutableDocument v = db.newDocument("V");
        v.set("id", 0);
        v.set("name", "Crash");
        v.set("surname", "Test");

        db.asynch().createRecord(v);
      }

      db.asynch().waitCompletion();

      Assertions.assertTrue(errors.get() > 0);

    } catch (TransactionException e) {
      Assertions.assertTrue(e.getCause() instanceof IOException);
    }
    ((DatabaseInternal) db).kill();

    verifyWALFilesAreStillPresent();

    verifyDatabaseWasNotClosedProperly();

    Assertions.assertEquals(TOT, database.countType("V", true));
  }

  private void verifyDatabaseWasNotClosedProperly() {
    final AtomicBoolean dbNotClosedCaught = new AtomicBoolean(false);

    factory.registerCallback(DatabaseInternal.CALLBACK_EVENT.DB_NOT_CLOSED, new Callable<Void>() {
      @Override
      public Void call() {
        dbNotClosedCaught.set(true);
        return null;
      }
    });

    database = factory.open();
    Assertions.assertTrue(dbNotClosedCaught.get());
  }

  private void verifyWALFilesAreStillPresent() {
    File dbDir = new File(getDatabasePath());
    Assertions.assertTrue(dbDir.exists());
    Assertions.assertTrue(dbDir.isDirectory());
    File[] files = dbDir.listFiles(new FilenameFilter() {
      @Override
      public boolean accept(File dir, String name) {
        return name.endsWith("wal");
      }
    });
    Assertions.assertTrue(files.length > 0);
  }
}