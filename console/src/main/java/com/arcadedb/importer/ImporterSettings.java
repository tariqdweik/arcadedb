/*
 * Copyright (c) 2018 - Arcade Analytics LTD (https://arcadeanalytics.com)
 */

package com.arcadedb.importer;

import com.arcadedb.utility.FileUtils;

import java.util.HashMap;
import java.util.Map;

public class ImporterSettings {
  String  database = "./databases/imported";
  boolean wal      = false;

  String documents;
  String documentTypeName = "Document";

  String vertices;
  String vertexTypeName = "Node";

  String  edges;
  String  edgeTypeName      = "Relationship";
  String  edgeFromField     = null;
  String  edgeToField       = null;
  boolean edgeBidirectional = true;

  String  typeIdProperty         = null;
  boolean typeIdPropertyIsUnique = false;
  String  typeIdType             = "String";
  int     commitEvery            = 5000;
  int     parallel               = Runtime.getRuntime().availableProcessors() / 2 - 1;
  boolean forceDatabaseCreate;
  boolean trimText               = true;
  Long    skipEntries            = null;
  long    analysisLimitBytes     = 100000;
  long    analysisLimitEntries   = 10000;
  long    parsingLimitBytes;
  long    parsingLimitEntries;

  final Map<String, String> options = new HashMap<>();

  protected void parseParameters(final String[] args) {
    if (args != null)
      for (int i = 0; i < args.length - 1; i += 2)
        parseParameter(args[i].substring(1), args[i + 1]);
  }

  public void parseParameter(final String name, final String value) {
    if ("database".equals(name))
      database = value;
    else if ("documents".equals(name))
      documents = value;
    else if ("forceDatabaseCreate".equals(name))
      forceDatabaseCreate = Boolean.parseBoolean(value);
    else if ("wal".equals(name))
      wal = Boolean.parseBoolean(value);
    else if ("commitEvery".equals(name))
      commitEvery = Integer.parseInt(value);
    else if ("parallel".equals(name))
      parallel = Integer.parseInt(value);
    else if ("documentType".equals(name))
      documentTypeName = value;
    else if ("vertices".equals(name))
      vertices = value;
    else if ("vertexType".equals(name))
      vertexTypeName = value;
    else if ("edges".equals(name))
      edges = value;
    else if ("edgeType".equals(name))
      edgeTypeName = value;
    else if ("edgeFromField".equals(name))
      edgeFromField = value;
    else if ("edgeToField".equals(name))
      edgeToField = value;
    else if ("edgeBidirectional".equals(name))
      edgeBidirectional = Boolean.parseBoolean(value);
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