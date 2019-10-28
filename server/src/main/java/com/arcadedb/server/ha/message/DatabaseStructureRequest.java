/*
 * Copyright (c) - Arcade Analytics LTD (https://arcadeanalytics.com)
 */
package com.arcadedb.server.ha.message;

import com.arcadedb.database.Binary;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.engine.PaginatedFile;
import com.arcadedb.network.binary.NetworkProtocolException;
import com.arcadedb.schema.SchemaImpl;
import com.arcadedb.server.ha.HAServer;
import com.arcadedb.utility.FileUtils;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class DatabaseStructureRequest extends HAAbstractCommand {
  private String databaseName;

  public DatabaseStructureRequest() {
  }

  public DatabaseStructureRequest(final String dbName) {
    this.databaseName = dbName;
  }

  @Override
  public HACommand execute(final HAServer server, final String remoteServerName, final long messageNumber) {
    final DatabaseInternal db = (DatabaseInternal) server.getServer().getDatabase(databaseName);

    final File file = new File(db.getDatabasePath() + "/" + SchemaImpl.SCHEMA_FILE_NAME);
    try {
      final String schemaJson;
      if (file.exists())
        schemaJson = FileUtils.readStreamAsString(new FileInputStream(file), db.getSchema().getEncoding());
      else
        schemaJson = "{}";

      final Map<Integer, String> fileNames = new HashMap<>();
      for (PaginatedFile f : db.getFileManager().getFiles())
        fileNames.put(f.getFileId(), f.getFileName());

      return new DatabaseStructureResponse(schemaJson, fileNames);

    } catch (IOException e) {
      throw new NetworkProtocolException("Error on reading schema json file", e);
    }
  }

  @Override
  public void toStream(final Binary stream) {
    stream.putString(databaseName);
  }

  @Override
  public void fromStream(final Binary stream) {
    databaseName = stream.getString();
  }

  @Override
  public String toString() {
    return "dbstructure(" + databaseName + ")";
  }
}
