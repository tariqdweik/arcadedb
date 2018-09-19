/*
 * Copyright (c) 2018 - Arcade Analytics LTD (https://arcadeanalytics.com)
 */

package com.arcadedb.importer;

import com.arcadedb.utility.FileUtils;

import java.util.HashMap;
import java.util.Map;

public class ImporterSettings {
  String               databaseURL            = "./databases/imported";
  String               url;
  Importer.RECORD_TYPE recordType             = Importer.RECORD_TYPE.DOCUMENT;
  String               edgeTypeName           = "Relationship";
  String               vertexTypeName         = "Node";
  String               typeIdProperty         = null;
  boolean              typeIdPropertyIsUnique = false;
  String               typeIdType             = "String";
  int                  commitEvery            = 1000;
  int                  parallel               = Runtime.getRuntime().availableProcessors() / 2 - 1;
  boolean              forceDatabaseCreate;
  boolean              trimText               = true;
  long                 limitBytes;
  long                 limitEntries;

  final Map<String, String> options = new HashMap<>();

  public void parseParameter(final String name, final String value) {
    if ("url".equals(name))
      url = value;
    else if ("database".equals(name))
      databaseURL = value;
    else if ("forceDatabaseCreate".equals(name))
      forceDatabaseCreate = Boolean.parseBoolean(value);
    else if ("commitEvery".equals(name))
      commitEvery = Integer.parseInt(value);
    else if ("parallel".equals(name))
      parallel = Integer.parseInt(value);
    else if ("vertexType".equals(name))
      vertexTypeName = value;
    else if ("edgeType".equals(name))
      edgeTypeName = value;
    else if ("id".equals(name))
      typeIdProperty = value;
    else if ("idUnique".equals(name))
      typeIdPropertyIsUnique = Boolean.parseBoolean(value);
    else if ("idType".equals(name))
      typeIdType = value;
    else if ("trimText".equals(name))
      trimText = Boolean.parseBoolean(value);
    else if ("limitBytes".equals(name))
      limitBytes = FileUtils.getSizeAsNumber(value);
    else if ("limitEntries".equals(name))
      limitEntries = Long.parseLong(value);
    else
      // ADDITIONAL OPTIONS
      options.put(name, value);
  }
}