/*
 * Copyright (c) 2018 - Arcade Analytics LTD (https://arcadeanalytics.com)
 */

package com.arcadedb.server.ha;

import com.arcadedb.database.Binary;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.engine.ModifiablePage;
import com.arcadedb.engine.WALFile;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;

/**
 * Copy the changes to WAL in memory to be propagated to other servers. The content in RAM is compressed.
 */
public class ReplicatedWALFile extends WALFile {
  private Binary changes = new Binary(4096);

  public ReplicatedWALFile(final String filePath) throws FileNotFoundException {
    super(filePath);
  }

  @Override
  public Binary writeTransaction(final DatabaseInternal database, final List<ModifiablePage> pages, final boolean sync, final WALFile file, final long txId)
      throws IOException {
    changes.clear();
    super.writeTransaction(database, pages, sync, file, txId);
    return changes.copy();
  }

  @Override
  protected void append(final ByteBuffer buffer) throws IOException {
    super.append(buffer);
    buffer.rewind();
    changes.putBuffer(buffer);
  }
}
