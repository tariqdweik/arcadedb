/*
 * Copyright (c) 2018 - Arcade Analytics LTD (https://arcadeanalytics.com)
 */

package com.arcadedb.index.lsm;

import com.arcadedb.database.Binary;
import com.arcadedb.database.Database;
import com.arcadedb.database.RID;
import com.arcadedb.engine.BasePage;
import com.arcadedb.engine.ModifiablePage;
import com.arcadedb.engine.PaginatedComponent;
import com.arcadedb.engine.PaginatedFile;
import com.arcadedb.index.Index;
import com.arcadedb.serializer.BinarySerializer;
import com.arcadedb.serializer.BinaryTypes;

import java.io.IOException;
import java.util.Set;

/**
 * Abstract class for LSM-based indexes.
 */
public abstract class IndexLSMAbstract extends PaginatedComponent implements Index {
  public static final int DEF_PAGE_SIZE     = 4 * 1024 * 1024;
  public static final RID REMOVED_ENTRY_RID = new RID(null, -1, -1l);

  protected final    BinarySerializer serializer;
  protected          byte[]           keyTypes;
  protected          byte             valueType;
  protected volatile boolean          compacting = false;
  protected final    boolean          unique;

  /**
   * Called at creation time.
   */
  protected IndexLSMAbstract(final Database database, final String name, final boolean unique, String filePath, final String ext, final PaginatedFile.MODE mode,
      final byte[] keyTypes, final int pageSize) throws IOException {
    super(database, name, filePath, database.getFileManager().newFileId(), ext, mode, pageSize);
    this.serializer = database.getSerializer();
    this.unique = unique;
    this.keyTypes = keyTypes;
    this.valueType = BinaryTypes.TYPE_COMPRESSED_RID;
  }

  /**
   * Called at cloning time.
   */
  protected IndexLSMAbstract(final Database database, final String name, final boolean unique, String filePath, final String ext, final byte[] keyTypes,
      final int pageSize) throws IOException {
    super(database, name, filePath, database.getFileManager().newFileId(), "temp_" + ext, PaginatedFile.MODE.READ_WRITE, pageSize);
    this.serializer = database.getSerializer();
    this.unique = unique;
    this.keyTypes = keyTypes;
    this.valueType = BinaryTypes.TYPE_COMPRESSED_RID;
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
    // TODO
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

  protected void setValuesFreePosition(final ModifiablePage currentPage, final int newValuesFreePosition) {
    currentPage.writeInt(0, newValuesFreePosition);
  }

  protected RID getRemovedRID(final RID rid) {
    return new RID(database, (rid.getBucketId() + 2) * -1, rid.getPosition());
  }

  protected RID getOriginalRID(final RID rid) {
    return new RID(database, (rid.getBucketId() * -1) - 2, rid.getPosition());
  }

  protected void updateEntryValue(final Binary buffer, final int valueIndex, final Object value) {
    final int items = (int) serializer.deserializeValue(database, buffer, BinaryTypes.TYPE_INT);

    if (valueIndex > items - 1)
      throw new IllegalArgumentException("Cannot update value index " + valueIndex + " in value container with only " + items + " items");

    // MOVE TO THE LAST ITEM
    buffer.position(buffer.position() + (BinaryTypes.getTypeSize(valueType) * valueIndex));

    // WRITE VALUES
    serializer.serializeValue(buffer, valueType, value);
  }
}
