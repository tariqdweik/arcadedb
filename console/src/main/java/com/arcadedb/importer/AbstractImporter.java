/*
 * Copyright (c) 2018 - Arcade Analytics LTD (https://arcadeanalytics.com)
 */

package com.arcadedb.importer;

import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.database.MutableDocument;
import com.arcadedb.utility.LogManager;

import java.io.*;
import java.util.Timer;
import java.util.TimerTask;
import java.util.zip.GZIPInputStream;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;

public abstract class AbstractImporter {
  private static final int DEFAULT_INPUT_BUFFER_SIZE = 128000;

  protected Database    database;
  protected InputStream inputFileStream;
  protected Reader      inputFileReader;
  protected long        inputFileSize;

  protected long parsed;
  protected long startedOn;

  protected long  lastLapOn;
  protected long  lastParsed;
  protected long  lastRecords;
  protected Timer timer;

  // SETTINGS
  protected String      databaseURL;
  protected String      inputFile;
  protected String      delimiter   = ",";
  protected RECORD_TYPE recordType  = RECORD_TYPE.DOCUMENT;
  protected String      typeName;
  protected int         commitEvery = 5000;
  protected boolean     forceDatabaseCreate;

  protected enum RECORD_TYPE {DOCUMENT, VERTEX}

  protected AbstractImporter(final String[] args) {
    parseParameters(args);
  }

  protected void printProgress() {
    long deltaInSecs = (System.currentTimeMillis() - lastLapOn) / 1000;
    if (deltaInSecs == 0)
      deltaInSecs = 1;

    final long lastRecordCount = database.countType(typeName, false);

    if (inputFileSize < 0) {
      LogManager.instance().info(this, "Parsed %d (%d/sec) - %d records (%d/sec)", parsed, ((parsed - lastParsed) / deltaInSecs), lastRecordCount,
          (lastRecordCount - lastRecords) / deltaInSecs);
    } else {
      final int progressPerc = (int) (getInputFilePosition() * 100 / inputFileSize);
      LogManager.instance()
          .info(this, "Parsed %d (%d/sec - %d%%) - %d records (%d/sec)", parsed, ((parsed - lastParsed) / deltaInSecs), progressPerc, lastRecordCount,
              (lastRecordCount - lastRecords) / deltaInSecs);
    }
    lastLapOn = System.currentTimeMillis();
    lastParsed = parsed;
    lastRecords = lastRecordCount;
  }

  protected abstract long getInputFilePosition();

  protected void startImporting() {
    startedOn = lastLapOn = System.currentTimeMillis();

    timer = new Timer();
    timer.schedule(new TimerTask() {
      @Override
      public void run() {
        printProgress();
      }
    }, 1000, 1000);
  }

  protected void stopImporting() {
    if (timer != null)
      timer.cancel();
    printProgress();
  }

  protected Reader openInputFile() throws IOException {
    File file = new File(inputFile);
    if (!file.exists())
      throw new IllegalArgumentException("Cannot browse input file '" + inputFile + "' because it does not exist");

    if (file.getName().endsWith(".zip")) {
      final ZipFile zip = new ZipFile(file);
      if (zip.size() != 1)
        throw new IllegalArgumentException("Cannot browse zipped input file '" + inputFile + "' because contains more than one file inside");
      final ZipEntry f = zip.entries().nextElement();
      inputFileStream = zip.getInputStream(f);
      inputFileSize = f.getSize();
    } else if (file.getName().endsWith(".gz")) {
      inputFileStream = new GZIPInputStream(new FileInputStream(file));
      inputFileSize = -1;
    } else {
      inputFileStream = new FileInputStream(file);
      inputFileSize = file.length();
    }

    inputFileStream = new BufferedInputStream(inputFileStream, DEFAULT_INPUT_BUFFER_SIZE);
    inputFileReader = new InputStreamReader(inputFileStream);

    return inputFileReader;
  }

  protected void closeInputFile() {
    try {
      if (inputFileStream != null)
        inputFileStream.close();

      if (inputFileReader != null)
        inputFileReader.close();
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  protected void closeDatabase() {
    if (database != null)
      database.close();
  }

  protected void openDatabase() {
    if (database != null && database.isOpen())
      throw new IllegalStateException("Database already open");
    final DatabaseFactory factory = new DatabaseFactory(databaseURL);

    if (forceDatabaseCreate) {
      if (factory.exists())
        factory.open().drop();
    }

    database = factory.exists() ? factory.open() : factory.create();

    if (!database.getSchema().existsType(typeName)) {
      LogManager.instance().info(this, "Creating type '%s' of type '%s'", typeName, recordType);
      switch (recordType) {
      case DOCUMENT:
        database.getSchema().createDocumentType(typeName);
        break;
      case VERTEX:
        database.getSchema().createVertexType(typeName);
        break;
      }
    }

    database.asynch().setParallelLevel(2);
    database.asynch().setCommitEvery(commitEvery);
  }

  protected MutableDocument createRecord() {
    switch (recordType) {
    case DOCUMENT:
      return database.newDocument(typeName);
    case VERTEX:
      return database.newVertex(typeName);
    default:
      throw new IllegalArgumentException("recordType '" + recordType + "' not supported");
    }
  }

  protected void parseParameters(final String[] args) {
    for (int i = 0; i < args.length - 1; i += 2)
      parseParameter(args[i], args[i + 1]);
  }

  protected void parseParameter(final String name, final String value) {
    if ("-file".equals(name)) {
      inputFile = value;
    } else if ("-database".equals(name)) {
      databaseURL = value;
    } else if ("-forceDatabaseCreate".equals(name)) {
      forceDatabaseCreate = Boolean.parseBoolean(value);
    } else if ("-delimiter".equals(name)) {
      delimiter = value;
    } else if ("-commitEvery".equals(name)) {
      commitEvery = Integer.parseInt(value);
    } else if ("-type".equals(name)) {
      typeName = value;
    } else if ("-recordType".equals(name)) {
      recordType = RECORD_TYPE.valueOf(value.toUpperCase());
    } else
      throw new IllegalArgumentException("Invalid setting '" + name + "'");
  }
}
