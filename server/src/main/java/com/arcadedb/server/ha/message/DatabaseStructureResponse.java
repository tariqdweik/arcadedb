/*
 * Copyright (c) 2018 - Arcade Analytics LTD (https://arcadeanalytics.com)
 */
package com.arcadedb.server.ha.message;

import com.arcadedb.database.Binary;
import com.arcadedb.server.ha.HAServer;

import java.util.HashMap;
import java.util.Map;

public class DatabaseStructureResponse extends HAAbstractCommand {
  private String               schemaJson;
  private Map<Integer, String> fileNames;

  public DatabaseStructureResponse() {
  }

  public DatabaseStructureResponse(final String schemaJson, final Map<Integer, String> fileNames) {
    this.schemaJson = schemaJson;
    this.fileNames = fileNames;
  }

  public Map<Integer, String> getFileNames() {
    return fileNames;
  }

  public String getSchemaJson() {
    return schemaJson;
  }

  @Override
  public void toStream(final Binary stream) {
    stream.putString(schemaJson);

    stream.putUnsignedNumber(fileNames.size());
    for (Map.Entry<Integer, String> file : fileNames.entrySet()) {
      stream.putInt(file.getKey());
      stream.putByte((byte) (file.getValue() != null ? 1 : 0));
      if (file.getValue() != null)
        stream.putString(file.getValue());
    }
  }

  @Override
  public void fromStream(final Binary stream) {
    schemaJson = stream.getString();

    fileNames = new HashMap<>();
    final int fileCount = (int) stream.getUnsignedNumber();
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
  public HACommand execute(final HAServer server, final String remoteServerName, final long messageNumber) {
    return null;
  }

  @Override
  public String toString() {
    return "dbstructure=" + fileNames;
  }
}
