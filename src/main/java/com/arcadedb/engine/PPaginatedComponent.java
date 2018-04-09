package com.arcadedb.engine;

import com.arcadedb.database.PDatabase;
import com.arcadedb.database.PDatabaseImpl;

import java.io.IOException;

/**
 * HEADER = [recordCount(int:4)] CONTENT-PAGES = [version(long:8),recordCountInPage(short:2),recordOffsetsInPage(512*ushort=2048)]
 */
public abstract class PPaginatedComponent {
  protected final PDatabaseImpl  database;
  protected final String         name;
  protected final PPaginatedFile file;
  protected final int            id;
  protected final int            pageSize;
  protected       int            pageCount;

  protected PPaginatedComponent(final PDatabase database, final String name, String filePath, final int id, final String ext,
      final PPaginatedFile.MODE mode, final int pageSize) throws IOException {
    this(database, name, filePath + "." + id + "." + pageSize + "." + ext, id, mode, pageSize);
  }

  protected PPaginatedComponent(final PDatabase database, final String name, String filePath, final int id,
      final PPaginatedFile.MODE mode, final int pageSize) throws IOException {
    this.database = (PDatabaseImpl) database;
    this.name = name;
    this.id = id;
    this.pageSize = pageSize;

    this.file = database.getFileManager().getOrCreateFile(name, filePath, mode);

    if (file.getSize() == 0)
      // NEW FILE, CREATE HEADER PAGE
      pageCount = 0;
    else
      pageCount = (int) (file.getSize() / getPageSize());
  }

  public int getPageSize() {
    return pageSize;
  }

  public void setPageCount(final int value) {
    assert value > pageCount;
    pageCount = value;
  }

  public String getName() {
    return name;
  }

  public int getId() {
    return id;
  }

  public PDatabaseImpl getDatabase() {
    return database;
  }

  public void drop() throws IOException {
    database.getSchema().removeFile(file.getFileId());
    database.getPageManager().deleteFile(file.getFileId());
    database.getFileManager().dropFile(file.getFileId());
  }

  protected int getTotalPages() {
    final Integer txPageCounter = database.getTransaction().getPageCounter(id);
    if (txPageCounter != null)
      return txPageCounter;
    return pageCount;
  }
}
