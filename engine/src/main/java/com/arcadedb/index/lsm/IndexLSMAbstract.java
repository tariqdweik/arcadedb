/*
 * Copyright (c) 2018 - Arcade Analytics LTD (https://arcadeanalytics.com)
 */

package com.arcadedb.index.lsm;

import com.arcadedb.database.Database;
import com.arcadedb.database.RID;
import com.arcadedb.engine.BasePage;
import com.arcadedb.engine.MutablePage;
import com.arcadedb.engine.PaginatedComponent;
import com.arcadedb.engine.PaginatedFile;
import com.arcadedb.index.Index;
import com.arcadedb.index.IndexException;
import com.arcadedb.serializer.BinarySerializer;
import com.arcadedb.serializer.BinaryTypes;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Set;

/**
 * Abstract class for LSM-based indexes.
 */
public abstract class IndexLSMAbstract extends PaginatedComponent implements Index {
  public static final  int    DEF_PAGE_SIZE     = 2 * 1024 * 1024;
  public static final  RID    REMOVED_ENTRY_RID = new RID(null, -1, -1l);
  private static final String TEMP_EXT          = "temp_";

  protected final    BinarySerializer  serializer;
  protected final    byte              valueType        = BinaryTypes.TYPE_COMPRESSED_RID;
  protected          byte[]            keyTypes;
  protected volatile COMPACTING_STATUS compactingStatus = COMPACTING_STATUS.NO;
  protected final    boolean           unique;

  public enum COMPACTING_STATUS {NO, IN_PROGRESS, COMPACTED}

  /**
   * Called at creation time.
   */
  protected IndexLSMAbstract(final Database database, final String name, final boolean unique, String filePath, final String ext, final PaginatedFile.MODE mode,
      final byte[] keyTypes, final int pageSize) throws IOException {
    super(database, name, filePath, database.getFileManager().newFileId(), ext, mode, pageSize);
    this.serializer = database.getSerializer();
    this.unique = unique;
    this.keyTypes = keyTypes;
  }

  /**
   * Called at cloning time.
   */
  protected IndexLSMAbstract(final Database database, final String name, final boolean unique, String filePath, final String ext, final byte[] keyTypes,
      final int pageSize) throws IOException {
    super(database, name, filePath, database.getFileManager().newFileId(), TEMP_EXT + ext, PaginatedFile.MODE.READ_WRITE, pageSize);
    this.serializer = database.getSerializer();
    this.unique = unique;
    this.keyTypes = keyTypes;
  }

  /**
   * Called at load time (1st page only).
   */
  protected IndexLSMAbstract(final Database database, final String name, final boolean unique, String filePath, final int id, final PaginatedFile.MODE mode,
      final int pageSize) throws IOException {
    super(database, name, filePath, id, mode, pageSize);
    this.serializer = database.getSerializer();
    this.unique = unique;
  }

  protected abstract void internalPut(Object[] keys, RID rid, boolean checkForUnique);

  protected abstract void internalRemove(Object[] keys, RID rid);

  public void removeTempSuffix() {
    final String fileName = file.getFileName();

    final int extPos = fileName.lastIndexOf('.');
    if (fileName.substring(extPos + 1).startsWith(TEMP_EXT)) {
      try {
        file.rename(fileName.substring(0, extPos) + "." + fileName.substring(extPos + TEMP_EXT.length() + 1));
      } catch (FileNotFoundException e) {
        throw new IndexException("Cannot rename temp file", e);
      }
    }
  }

  @Override
  public Set<RID> get(final Object[] keys) {
    return get(keys, -1);
  }

  @Override
  public void put(final Object[] keys, final RID rid) {
    put(keys, rid, true);
  }

  @Override
  public void put(final Object[] keys, final RID rid, final boolean checkForUnique) {
    if (rid == null)
      throw new IllegalArgumentException("RID is null");

    internalPut(keys, rid, checkForUnique);
  }

  @Override
  public void remove(final Object[] keys, final RID rid) {
    internalRemove(keys, rid);
  }

  public boolean isUnique() {
    return unique;
  }

  @Override
  public int getFileId() {
    return file.getFileId();
  }

  @Override
  public String toString() {
    return name;
  }

  public byte[] getKeyTypes() {
    return keyTypes;
  }

  public boolean isDeletedEntry(final Object rid) {
    return ((RID) rid).getBucketId() < 0;
  }

  protected Object[] checkForNulls(final Object keys[]) {
    if (keys != null)
      for (int i = 0; i < keys.length; ++i)
        if (keys[i] == null)
          throw new IllegalArgumentException("Indexed key cannot be NULL");
    return keys;
  }

  protected int getValuesFreePosition(final BasePage currentPage) {
    return currentPage.readInt(0);
  }

  protected void setValuesFreePosition(final MutablePage currentPage, final int newValuesFreePosition) {
    currentPage.writeInt(0, newValuesFreePosition);
  }

  protected RID getRemovedRID(final RID rid) {
    return new RID(database, (rid.getBucketId() + 2) * -1, rid.getPosition());
  }

  protected RID getOriginalRID(final RID rid) {
    return new RID(database, (rid.getBucketId() * -1) - 2, rid.getPosition());
  }
}
