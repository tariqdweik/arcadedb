/*
 * Copyright (c) 2018 - Arcade Analytics LTD (https://arcadeanalytics.com)
 */

package com.arcadedb;

import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.ModifiableDocument;
import com.arcadedb.database.async.ErrorCallback;
import com.arcadedb.engine.PaginatedFile;
import com.arcadedb.engine.WALException;
import com.arcadedb.exception.TransactionException;
import com.arcadedb.utility.FileUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

public class ACIDTransactionTest {
  private static final int    TOT     = 10000;
  private static final String DB_PATH = "target/database/testdb";

  @BeforeEach
  public void populate() {
    FileUtils.deleteRecursively(new File(DB_PATH));
    new DatabaseFactory(DB_PATH, PaginatedFile.MODE.READ_WRITE).execute(new DatabaseFactory.POperation() {
      @Override
      public void execute(Database database) {
        if (!database.getSchema().existsType("V"))
          database.getSchema().createDocumentType("V");
      }
    });
  }

  @AfterEach
  public void drop() {
    final Database db = new DatabaseFactory(DB_PATH, PaginatedFile.MODE.READ_WRITE).open();
    db.drop();
  }

  @Test
  public void testCrashDuringTx() {
    final Database db = new DatabaseFactory(DB_PATH, PaginatedFile.MODE.READ_WRITE).open();
    db.begin();
    try {
      final ModifiableDocument v = db.newDocument("V");
      v.set("id", 0);
      v.set("name", "Crash");
      v.set("surname", "Test");
      v.save();

    } finally {
      ((DatabaseInternal) db).kill();
    }

    final Database db2 = verifyDatabaseWasNotClosedProperly();
    try {
      Assertions.assertEquals(0, db2.countType("V", true));
    } finally {
      db.close();
    }
  }

  @Test
  public void testIOExceptionDuringCommit() {
    final Database db = new DatabaseFactory(DB_PATH, PaginatedFile.MODE.READ_WRITE).open();
    db.begin();

    try {
      final ModifiableDocument v = db.newDocument("V");
      v.set("id", 0);
      v.set("name", "Crash");
      v.set("surname", "Test");
      v.save();

      ((DatabaseInternal) db).registerCallback(DatabaseInternal.CALLBACK_EVENT.TX_LAST_OP, new Callable<Void>() {
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

    final Database db2 = verifyDatabaseWasNotClosedProperly();
    try {
      Assertions.assertEquals(0, db2.countType("V", true));
    } finally {
      db2.close();
    }
  }

  @Test
  public void testIOExceptionAfterWALIsWritten() {
    final Database db = new DatabaseFactory(DB_PATH, PaginatedFile.MODE.READ_WRITE).open();
    db.begin();

    try {
      final ModifiableDocument v = db.newDocument("V");
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

    final Database db2 = verifyDatabaseWasNotClosedProperly();
    try {
      Assertions.assertEquals(1, db2.countType("V", true));
    } finally {
      db2.close();
    }
  }

  @Test
  public void testAsyncIOExceptionAfterWALIsWrittenLastRecords() {
    final Database db = new DatabaseFactory(DB_PATH, PaginatedFile.MODE.READ_WRITE).open();

    final AtomicInteger errors = new AtomicInteger(0);

    db.asynch().setTransactionSync(true);
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
          if (commits.incrementAndGet() > TOT - 1)
            throw new IOException("Test IO Exception");
          return null;
        }
      });

      for (; total.get() < TOT; total.incrementAndGet()) {
        final ModifiableDocument v = db.newDocument("V");
        v.set("id", 0);
        v.set("name", "Crash");
        v.set("surname", "Test");

        db.asynch().createRecord(v);
      }

      db.asynch().waitCompletion();

      Assertions.assertEquals(1, errors.get());

    } catch (TransactionException e) {
      Assertions.assertTrue(e.getCause() instanceof IOException);
    }
    ((DatabaseInternal) db).kill();

    verifyWALFilesAreStillPresent();

    final Database db2 = verifyDatabaseWasNotClosedProperly();
    try {
      Assertions.assertEquals(TOT, db2.countType("V", true));
    } finally {
      db2.close();
    }
  }

  @Test
  public void testAsyncIOExceptionAfterWALIsWrittenManyRecords() {
    final Database db = new DatabaseFactory(DB_PATH, PaginatedFile.MODE.READ_WRITE).open();

    final int TOT = 100000;

    final AtomicInteger total = new AtomicInteger(0);

    final AtomicInteger errors = new AtomicInteger(0);

    db.asynch().setTransactionSync(true);
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

      db.asynch().onError(new ErrorCallback() {
        @Override
        public void call(Exception exception) {
          errors.incrementAndGet();
        }
      });

      for (; total.get() < TOT; total.incrementAndGet()) {
        final ModifiableDocument v = db.newDocument("V");
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

    final Database db2 = verifyDatabaseWasNotClosedProperly();
    try {
      Assertions.assertEquals(TOT, db2.countType("V", true));
    } finally {
      db2.close();
    }
  }

  private Database verifyDatabaseWasNotClosedProperly() {
    final AtomicBoolean dbNotClosedCaught = new AtomicBoolean(false);

    final DatabaseFactory factory = new DatabaseFactory(DB_PATH, PaginatedFile.MODE.READ_WRITE);
    factory.registerCallback(DatabaseInternal.CALLBACK_EVENT.DB_NOT_CLOSED, new Callable<Void>() {
      @Override
      public Void call() {
        dbNotClosedCaught.set(true);
        return null;
      }
    });

    Database db = factory.open();
    Assertions.assertTrue(dbNotClosedCaught.get());
    return db;
  }

  private void verifyWALFilesAreStillPresent() {
    File dbDir = new File(DB_PATH);
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