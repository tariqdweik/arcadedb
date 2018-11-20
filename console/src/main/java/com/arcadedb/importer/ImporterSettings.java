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
  String               documentTypeName       = "Document";
  String               vertexTypeName         = "Node";
  String               edgeTypeName           = "Relationship";
  String               edgeFromField          = null;
  String               edgeToField            = null;
  String               typeIdProperty         = null;
  boolean              typeIdPropertyIsUnique = false;
  String               typeName;
  String               typeIdType             = "String";
  int                  commitEvery            = 1000;
  int                  parallel               = Runtime.getRuntime().availableProcessors() / 2 - 1;
  boolean              forceDatabaseCreate;
  boolean              trimText               = true;
  Long                 skipEntries            = null;
  long                 analysisLimitBytes     = 100000;
  long                 analysisLimitEntries   = 10000;
  long                 parsingLimitBytes;
  long                 parsingLimitEntries;

  final Map<String, String> options = new HashMap<>();

  protected void parseParameters(final String[] args) {
    if (args != null)
      for (int i = 0; i < args.length - 1; i += 2)
        parseParameter(args[i].substring(1), args[i + 1]);

    switch (recordType) {
    case VERTEX:
      typeName = vertexTypeName;
      break;

    case EDGE:
      typeName = edgeTypeName;
      break;

    case DOCUMENT:
      typeName = documentTypeName;
      break;

    default:
      throw new IllegalArgumentException("Record type " + recordType + " not supported");
    }
  }

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
    else if ("recordType".equals(name))
      recordType = Importer.RECORD_TYPE.valueOf(value.toUpperCase());
    else if ("documentType".equals(name))
      documentTypeName = value;
    else if ("vertexType".equals(name))
      vertexTypeName = value;
    else if ("edgeType".equals(name))
      edgeTypeName = value;
    else if ("edgeFromField".equals(name))
      edgeFromField = value;
    else if ("edgeToField".equals(name))
      edgeToField = value;
    else if ("typeIdProperty".equals(name))
      typeIdProperty = value;
    else if ("typeIdUnique".equals(name))
      typeIdPropertyIsUnique = Boolean.parseBoolean(value);
    else if ("typeIdType".equals(name))
      typeIdType = value;
    else if ("trimText".equals(name))
      trimText = Boolean.parseBoolean(value);
    else if ("skipEntries".equals(name))
      skipEntries = FileUtils.getSizeAsNumber(value);
    else if ("analysisLimitBytes".equals(name))
      analysisLimitBytes = FileUtils.getSizeAsNumber(value);
    else if ("analysisLimitEntries".equals(name))
      analysisLimitEntries = Long.parseLong(value);
    else if ("parsingLimitBytes".equals(name))
      parsingLimitBytes = FileUtils.getSizeAsNumber(value);
    else if ("parsingLimitEntries".equals(name))
      parsingLimitEntries = Long.parseLong(value);
    else
      // ADDITIONAL OPTIONS
      options.put(name, value);
  }
}