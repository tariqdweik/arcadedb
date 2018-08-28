/*
 * Copyright (c) 2018 - Arcade Analytics LTD (https://arcadeanalytics.com)
 */

package com.arcadedb.engine;

import com.arcadedb.database.Database;
import com.arcadedb.database.EmbeddedDatabase;
import com.arcadedb.database.TransactionContext;
import com.arcadedb.schema.SchemaImpl;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * HEADER = [recordCount(int:4)] CONTENT-PAGES = [version(long:8),recordCountInPage(short:2),recordOffsetsInPage(512*ushort=2048)]
 */
public abstract class PaginatedComponent {
  protected final EmbeddedDatabase database;
  protected final String           name;
  protected final PaginatedFile    file;
  protected final int              id;
  protected final int              pageSize;
  protected final AtomicInteger    pageCount = new AtomicInteger();

  protected PaginatedComponent(final Database database, final String name, String filePath, final String ext, final PaginatedFile.MODE mode, final int pageSize)
      throws IOException {
    this(database, name, filePath, ext, database.getFileManager().newFileId(), mode, pageSize);
  }

  protected PaginatedComponent(final Database database, final String name, String filePath, final int id, final PaginatedFile.MODE mode, final int pageSize)
      throws IOException {
    this.database = (EmbeddedDatabase) database;
    this.name = name;
    this.id = id;
    this.pageSize = pageSize;

    this.file = database.getFileManager().getOrCreateFile(name, filePath, mode);

    if (file.getSize() == 0)
      // NEW FILE, CREATE HEADER PAGE
      pageCount.set(0);
    else
      pageCount.set((int) (file.getSize() / getPageSize()));
  }

  private PaginatedComponent(final Database database, final String name, String filePath, final String ext, final int id, final PaginatedFile.MODE mode,
      final int pageSize) throws IOException {
    this(database, name, filePath + "." + id + "." + pageSize + "." + ext, id, mode, pageSize);
  }

  public void onAfterLoad() {
  }

  public int getPageSize() {
    return pageSize;
  }

  public void setPageCount(final int value) {
    assert value > pageCount.get();
    pageCount.set(value);
  }

  public String getName() {
    return name;
  }

  public int getId() {
    return id;
  }

  public EmbeddedDatabase getDatabase() {
    return database;
  }

  public void drop() throws IOException {
    if (database.isOpen()) {
      ((SchemaImpl) database.getSchema()).removeFile(file.getFileId());
      database.getPageManager().deleteFile(file.getFileId());
      database.getFileManager().dropFile(file.getFileId());
    } else {
      new File(file.getFilePath()).delete();
    }
  }

  public int getTotalPages() {
    final TransactionContext tx = database.getTransaction();
    if (tx != null) {
      final Integer txPageCounter = tx.getPageCounter(id);
      if (txPageCounter != null)
        return txPageCounter;
    }
    return pageCount.get();
  }

  public Object getMainComponent() {
    return this;
  }
}
