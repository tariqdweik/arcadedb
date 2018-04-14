package com.arcadedb;

import com.arcadedb.database.PDatabase;
import com.arcadedb.database.PDatabaseFactory;
import com.arcadedb.database.PDatabaseInternal;
import com.arcadedb.database.PModifiableDocument;
import com.arcadedb.database.async.PErrorCallback;
import com.arcadedb.engine.PPaginatedFile;
import com.arcadedb.engine.PWALException;
import com.arcadedb.exception.PTransactionException;
import com.arcadedb.utility.PFileUtils;
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
    PFileUtils.deleteRecursively(new File(DB_PATH));
    new PDatabaseFactory(DB_PATH, PPaginatedFile.MODE.READ_WRITE).execute(new PDatabaseFactory.POperation() {
      @Override
      public void execute(PDatabase database) {
        if (!database.getSchema().existsType("V"))
          database.getSchema().createDocumentType("V");
      }
    });
  }

  @AfterEach
  public void drop() {
    final PDatabase db = new PDatabaseFactory(DB_PATH, PPaginatedFile.MODE.READ_WRITE).acquire();
    db.drop();
  }

  @Test
  public void testCrashDuringTx() {
    final PDatabase db = new PDatabaseFactory(DB_PATH, PPaginatedFile.MODE.READ_WRITE).acquire();
    db.begin();
    try {
      final PModifiableDocument v = db.newDocument("V");
      v.set("id", 0);
      v.set("name", "Crash");
      v.set("surname", "Test");
      v.save();

    } finally {
      ((PDatabaseInternal) db).kill();
    }

    final PDatabase db2 = verifyDatabaseWasNotClosedProperly();
    try {
      Assertions.assertEquals(0, db2.countType("V"));
    } finally {
      db.close();
    }
  }

  @Test
  public void testIOExceptionDuringCommit() {
    final PDatabase db = new PDatabaseFactory(DB_PATH, PPaginatedFile.MODE.READ_WRITE).acquire();
    db.begin();

    try {
      final PModifiableDocument v = db.newDocument("V");
      v.set("id", 0);
      v.set("name", "Crash");
      v.set("surname", "Test");
      v.save();

      ((PDatabaseInternal) db).registerCallback(PDatabaseInternal.CALLBACK_EVENT.TX_LAST_OP, new Callable<Void>() {
        @Override
        public Void call() throws IOException {
          throw new IOException("Test IO Exception");
        }
      });

      db.commit();

      Assertions.fail("Expected commit to fail");

    } catch (PTransactionException e) {
      Assertions.assertTrue(e.getCause() instanceof PWALException);
    }
    ((PDatabaseInternal) db).kill();

    verifyWALFilesAreStillPresent();

    final PDatabase db2 = verifyDatabaseWasNotClosedProperly();
    try {
      Assertions.assertEquals(0, db2.countType("V"));
    } finally {
      db2.close();
    }
  }

  @Test
  public void testIOExceptionAfterWALIsWritten() {
    final PDatabase db = new PDatabaseFactory(DB_PATH, PPaginatedFile.MODE.READ_WRITE).acquire();
    db.begin();

    try {
      final PModifiableDocument v = db.newDocument("V");
      v.set("id", 0);
      v.set("name", "Crash");
      v.set("surname", "Test");
      v.save();

      ((PDatabaseInternal) db).registerCallback(PDatabaseInternal.CALLBACK_EVENT.TX_AFTER_WAL_WRITE, new Callable<Void>() {
        @Override
        public Void call() throws IOException {
          throw new IOException("Test IO Exception");
        }
      });

      db.commit();

      Assertions.fail("Expected commit to fail");

    } catch (PTransactionException e) {
      Assertions.assertTrue(e.getCause() instanceof PWALException);
    }
    ((PDatabaseInternal) db).kill();

    verifyWALFilesAreStillPresent();

    final PDatabase db2 = verifyDatabaseWasNotClosedProperly();
    try {
      Assertions.assertEquals(1, db2.countType("V"));
    } finally {
      db2.close();
    }
  }

  @Test
  public void testAsyncIOExceptionAfterWALIsWrittenLastRecords() {
    final PDatabase db = new PDatabaseFactory(DB_PATH, PPaginatedFile.MODE.READ_WRITE).acquire();

    final AtomicInteger errors = new AtomicInteger(0);

    db.asynch().setTransactionSync(true);
    db.asynch().setTransactionUseWAL(true);
    db.asynch().setCommitEvery(1);
    db.asynch().onError(new PErrorCallback() {
      @Override
      public void call(Exception exception) {
        errors.incrementAndGet();
      }
    });

    final int TOT = 1000;

    final AtomicInteger total = new AtomicInteger(0);
    final AtomicInteger commits = new AtomicInteger(0);

    try {
      ((PDatabaseInternal) db).registerCallback(PDatabaseInternal.CALLBACK_EVENT.TX_AFTER_WAL_WRITE, new Callable<Void>() {
        @Override
        public Void call() throws IOException {
          if (commits.incrementAndGet() > TOT - 1)
            throw new IOException("Test IO Exception");
          return null;
        }
      });

      for (; total.get() < TOT; total.incrementAndGet()) {
        final PModifiableDocument v = db.newDocument("V");
        v.set("id", 0);
        v.set("name", "Crash");
        v.set("surname", "Test");

        db.asynch().createRecord(v);
      }

      db.asynch().waitCompletion();

      Assertions.assertEquals(1, errors.get());

    } catch (PTransactionException e) {
      Assertions.assertTrue(e.getCause() instanceof IOException);
    }
    ((PDatabaseInternal) db).kill();

    verifyWALFilesAreStillPresent();

    final PDatabase db2 = verifyDatabaseWasNotClosedProperly();
    try {
      Assertions.assertEquals(TOT, db2.countType("V"));
    } finally {
      db2.close();
    }
  }

  @Test
  public void testAsyncIOExceptionAfterWALIsWrittenManyRecords() {
    final PDatabase db = new PDatabaseFactory(DB_PATH, PPaginatedFile.MODE.READ_WRITE).acquire();

    final int TOT = 100000;

    final AtomicInteger total = new AtomicInteger(0);

    final AtomicInteger errors = new AtomicInteger(0);

    db.asynch().setTransactionSync(true);
    db.asynch().setTransactionUseWAL(true);
    db.asynch().setCommitEvery(1000000);
    db.asynch().onError(new PErrorCallback() {
      @Override
      public void call(Exception exception) {
        errors.incrementAndGet();
      }
    });

    try {
      ((PDatabaseInternal) db).registerCallback(PDatabaseInternal.CALLBACK_EVENT.TX_AFTER_WAL_WRITE, new Callable<Void>() {
        @Override
        public Void call() throws IOException {
          if (total.incrementAndGet() > TOT - 10)
            throw new IOException("Test IO Exception");
          return null;
        }
      });

      db.asynch().onError(new PErrorCallback() {
        @Override
        public void call(Exception exception) {
          errors.incrementAndGet();
        }
      });

      for (; total.get() < TOT; total.incrementAndGet()) {
        final PModifiableDocument v = db.newDocument("V");
        v.set("id", 0);
        v.set("name", "Crash");
        v.set("surname", "Test");

        db.asynch().createRecord(v);
      }

      db.asynch().waitCompletion();

      Assertions.assertTrue(errors.get() > 0);

    } catch (PTransactionException e) {
      Assertions.assertTrue(e.getCause() instanceof IOException);
    }
    ((PDatabaseInternal) db).kill();

    verifyWALFilesAreStillPresent();

    final PDatabase db2 = verifyDatabaseWasNotClosedProperly();
    try {
      Assertions.assertEquals(TOT, db2.countType("V"));
    } finally {
      db2.close();
    }
  }

  private PDatabase verifyDatabaseWasNotClosedProperly() {
    final AtomicBoolean dbNotClosedCaught = new AtomicBoolean(false);

    final PDatabaseFactory factory = new PDatabaseFactory(DB_PATH, PPaginatedFile.MODE.READ_WRITE);
    factory.registerCallback(PDatabaseInternal.CALLBACK_EVENT.DB_NOT_CLOSED, new Callable<Void>() {
      @Override
      public Void call() {
        dbNotClosedCaught.set(true);
        return null;
      }
    });

    PDatabase db = factory.acquire();
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