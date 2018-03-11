package com.arcadedb.engine;

import com.arcadedb.database.PDatabase;
import com.arcadedb.database.PDatabaseImpl;

import java.io.IOException;

/**
 * HEADER = [recordCount(int:4)] CONTENT-PAGES = [version(long:8),recordCountInPage(short:2),recordOffsetsInPage(512*ushort=2048)]
 */
public abstract class PPaginatedFile {
  protected final PDatabaseImpl database;
  protected final String        name;
  protected final PFile         file;
  protected final int           id;
  protected       int           pageCount;

  protected PPaginatedFile(final PDatabase database, final String name, String filePath, final int id, final String ext,
      final PFile.MODE mode) throws IOException {
    this(database, name, filePath + "." + id + "." + ext, id, mode);
  }

  protected PPaginatedFile(final PDatabase database, final String name, String filePath, final int id, final PFile.MODE mode)
      throws IOException {
    this.database = (PDatabaseImpl) database;
    this.name = name;
    this.id = id;

    this.file = database.getFileManager().getOrCreateFile(name, filePath, mode);

    if (file.getSize() == 0)
      // NEW FILE, CREATE HEADER PAGE
      pageCount = 0;
    else
      pageCount = (int) (file.getSize() / getPageSize());
  }

  protected abstract int getPageSize();

  public void onAfterCommit(final int value) {
    assert value > pageCount;
    pageCount = value;
  }

  public String getName() {
    return name;
  }

  public int getId() {
    return id;
  }

  public void flush() throws IOException {
    database.getPageManager().flushFile(file.getFileId());
  }

  public void drop() throws IOException {
    database.getSchema().removeFile(file.getFileId());
    database.getPageManager().disposeFile(file.getFileId());
    database.getFileManager().dropFile(file.getFileId());
  }
}
