/*
 * Copyright (c) 2018 - Arcade Analytics LTD (https://arcadeanalytics.com)
 */
package com.arcadedb.server.ha.message;

import com.arcadedb.database.Binary;
import com.arcadedb.database.Database;
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

public class DatabaseStructureResponse implements HAResponseMessage<DatabaseStructureRequest> {
  public final static byte                 ID = DatabaseStructureRequest.ID;
  private             String               schemaJson;
  private             Map<Integer, String> fileNames;

  public DatabaseStructureResponse() {
  }

  public Map<Integer, String> getFileNames() {
    return fileNames;
  }

  public String getSchemaJson() {
    return schemaJson;
  }

  @Override
  public void toStream(final Binary stream) {
    stream.putByte(ID);

    stream.putString(schemaJson);

    stream.putNumber(fileNames.size());
    for (Map.Entry<Integer, String> file : fileNames.entrySet()) {
      stream.putInt(file.getKey());
      stream.putByte((byte) (file.getValue() != null ? 1 : 0));
      if (file.getValue() != null)
        stream.putString(file.getValue());
    }
  }

  @Override
  public void fromStream(final Binary stream) {
    // SKIP THE COMMAND ID
    stream.getByte();

    schemaJson = stream.getString();

    fileNames = new HashMap<>();
    final int fileCount = (int) stream.getNumber();
    for (int i = 0; i < fileCount; ++i) {
      final int fileId = stream.getInt();
      final boolean notNull = stream.getByte() == 1;
      if (notNull)
        fileNames.put(fileId, stream.getString());
      else
        fileNames.put(fileId, null);
    }
  }

  @Override
  public void build(final HAServer server, final DatabaseStructureRequest request) {
    final Database db = server.getServer().getDatabase(request.getDatabaseName());

    final File file = new File(db.getDatabasePath() + "/" + SchemaImpl.SCHEMA_FILE_NAME);
    try {
      schemaJson = FileUtils.readStreamAsString(new FileInputStream(file), db.getSchema().getEncoding());

      fileNames = new HashMap<>();
      for (PaginatedFile f : db.getFileManager().getFiles())
        fileNames.put(f.getFileId(), f.getFileName());

    } catch (IOException e) {
      throw new NetworkProtocolException("Error on reading schema json file");
    }
  }

  @Override
  public String toString() {
    return "dbstructure=" + fileNames;
  }
}
