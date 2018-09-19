/*
 * Copyright (c) 2018 - Arcade Analytics LTD (https://arcadeanalytics.com)
 */

package com.arcadedb.importer;

import com.arcadedb.database.*;
import com.arcadedb.engine.Dictionary;
import com.arcadedb.schema.*;
import com.arcadedb.utility.LogManager;

import java.util.Map;
import java.util.Timer;
import java.util.TimerTask;

public abstract class AbstractImporter {
  private static final char[] STRING_CONTENT_SKIP = new char[] { '\'', '\'', '"', '"' };

  protected Database database;
  protected Source   source;

  protected long parsed;
  protected long startedOn;

  protected long createdVertices;
  protected long createdEdges;
  protected long createdDocuments;

  protected long  lastLapOn;
  protected long  lastParsed;
  protected long  lastDocuments;
  protected long  lastVertices;
  protected long  lastEdges;
  protected Timer timer;

  // SETTINGS
  protected String      databaseURL            = "./databases/imported";
  protected String      url;
  protected RECORD_TYPE recordType             = RECORD_TYPE.DOCUMENT;
  protected String      edgeTypeName           = "Relationship";
  protected String      vertexTypeName         = "Node";
  protected String      typeIdProperty         = null;
  private   boolean     typeIdPropertyIsUnique = false;
  private   String      typeIdType             = "String";
  protected int         commitEvery            = 1000;
  protected int         parallel               = Runtime.getRuntime().availableProcessors() / 2 - 1;
  protected boolean     forceDatabaseCreate;

  protected enum RECORD_TYPE {DOCUMENT, VERTEX}

  protected AbstractImporter(final String[] args) {
    parseParameters(args);
  }

  protected void printProgress() {
    long deltaInSecs = (System.currentTimeMillis() - lastLapOn) / 1000;
    if (deltaInSecs == 0)
      deltaInSecs = 1;

    if (source == null || source.totalSize < 0) {
      LogManager.instance()
          .info(this, "Parsed %d (%d/sec) - %d documents (%d/sec) - %d vertices (%d/sec) - %d edges (%d/sec)", parsed, ((parsed - lastParsed) / deltaInSecs),
              createdDocuments, (createdDocuments - lastDocuments) / deltaInSecs, createdVertices, (createdVertices - lastVertices) / deltaInSecs, createdEdges,
              (createdEdges - lastEdges) / deltaInSecs);
    } else {
      final int progressPerc = (int) (getInputFilePosition() * 100 / source.totalSize);
      LogManager.instance().info(this, "Parsed %d (%d/sec - %d%%) - %d records (%d/sec) - %d vertices (%d/sec) - %d edges (%d/sec)", parsed,
          ((parsed - lastParsed) / deltaInSecs), progressPerc, createdDocuments, (createdDocuments - lastDocuments) / deltaInSecs, createdVertices,
          (createdVertices - lastVertices) / deltaInSecs, createdEdges, (createdEdges - lastEdges) / deltaInSecs);
    }
    lastLapOn = System.currentTimeMillis();
    lastParsed = parsed;

    lastDocuments = createdDocuments;
    lastVertices = createdVertices;
    lastEdges = createdEdges;
  }

  protected abstract long getInputFilePosition();

  protected void startImporting() {
    startedOn = lastLapOn = System.currentTimeMillis();

    timer = new Timer();
    timer.schedule(new TimerTask() {
      @Override
      public void run() {
        printProgress();
      }
    }, 5000, 5000);
  }

  protected void stopImporting() {
    if (timer != null)
      timer.cancel();
    printProgress();
  }

  protected void closeInputFile() {
    if (source != null)
      source.close();
  }

  protected void closeDatabase() {
    if (database != null) {
      if (database.isTransactionActive())
        database.commit();
      database.close();
    }
  }

  protected void openDatabase() {
    if (database != null && database.isOpen())
      throw new IllegalStateException("Database already open");

    final DatabaseFactory factory = new DatabaseFactory(databaseURL);

    if (forceDatabaseCreate) {
      if (factory.exists())
        factory.open().drop();
    }

    database = factory.exists() ? factory.open() : factory.create();

    database.begin();
    vertexTypeName = getOrCreateVertexType(vertexTypeName).getName();
    edgeTypeName = getOrCreateEdgeType(edgeTypeName).getName();
    database.commit();

    database.setReadYourWrites(false);
    database.asynch().setParallelLevel(parallel);
    database.asynch().setCommitEvery(commitEvery);

    database.begin();
  }

  protected Cursor<RID> lookupRecord(final String typeName, final Object id) {
    return database.lookupByKey(typeName, new String[] { typeIdProperty }, new Object[] { id });
  }

  protected MutableDocument createRecord(final RECORD_TYPE recordType, final String typeName) {
    switch (recordType) {
    case DOCUMENT:
      return database.newDocument(typeName);
    case VERTEX:
      return database.newVertex(typeName);
    default:
      throw new IllegalArgumentException("recordType '" + recordType + "' not supported");
    }
  }

  protected void parseParameters(final String[] args) {
    if (args != null)
      for (int i = 0; i < args.length - 1; i += 2)
        parseParameter(args[i], args[i + 1]);
  }

  protected String getStringContent(final String value) {
    return getStringContent(value, STRING_CONTENT_SKIP);
  }

  protected String getStringContent(final String value, final char[] chars) {
    if (value.length() > 1) {
      final char begin = value.charAt(0);

      for (int i = 0; i < chars.length - 1; i += 2) {
        if (begin == chars[i]) {
          final char end = value.charAt(value.length() - 1);
          if (end == chars[i + 1])
            return value.substring(1, value.length() - 1);
        }
      }
    }
    return value;
  }

  protected void beginTxIfNeeded() {
    if (!database.isTransactionActive())
      database.begin();
  }

  protected DocumentType getOrCreateDocumentType(final String name) {
    if (!database.getSchema().existsType(name)) {
      LogManager.instance().info(this, "Creating type '%s' of type DOCUMENT", name);

      beginTxIfNeeded();
      final DocumentType type = database.getSchema().createDocumentType(name, parallel);
      if (typeIdProperty != null) {
        type.createProperty(typeIdProperty, Type.getTypeByName(typeIdType));
        database.getSchema().createClassIndexes(SchemaImpl.INDEX_TYPE.LSM_TREE, typeIdPropertyIsUnique, name, new String[] { typeIdProperty });
      }
      return type;
    }
    return database.getSchema().getType(name);
  }

  protected VertexType getOrCreateVertexType(final String name) {
    final String realName = "v_" + name;
    if (!database.getSchema().existsType(realName)) {
      LogManager.instance().info(this, "Creating type '%s' of type VERTEX", name);

      beginTxIfNeeded();
      final VertexType type = database.getSchema().createVertexType(realName, parallel);
      if (typeIdProperty != null) {
        type.createProperty(typeIdProperty, Type.getTypeByName(typeIdType));
        database.getSchema().createClassIndexes(SchemaImpl.INDEX_TYPE.LSM_TREE, typeIdPropertyIsUnique, realName, new String[] { typeIdProperty });
      }
      return type;
    }

    return (VertexType) database.getSchema().getType(realName);
  }

  protected EdgeType getOrCreateEdgeType(final String name) {
    final String realName = "e_" + name;
    if (!database.getSchema().existsType(realName)) {
      LogManager.instance().info(this, "Creating type '%s' of type EDGE", name);

      beginTxIfNeeded();
      return database.getSchema().createEdgeType(realName, parallel);
    }

    return (EdgeType) database.getSchema().getType(realName);
  }

  protected void updateDatabaseSchema(final AnalyzedSchema schema) {
    if (schema == null)
      return;

    final Dictionary dictionary = database.getSchema().getDictionary();

    LogManager.instance().info(this, "Checking schema...");

    for (String entity : schema.getEntities()) {
      final VertexType type = getOrCreateVertexType(entity);

      for (Map.Entry<String, AnalyzedProperty> p : schema.getProperties(entity)) {
        final String propName = p.getKey();
        final AnalyzedProperty propValue = p.getValue();

        if (type.existsProperty(propName)) {
          // CHECK TYPE
          final Property property = type.getProperty(propName);
          if (property.getType() != propValue.getType()) {
            LogManager.instance()
                .warn(this, "- found schema property %s.%s of type %s, while analyzing the source type %s was found", entity, propName, property.getType(),
                    propValue.getType());
          }
        } else {
          // CREATE IT
          LogManager.instance().info(this, "- creating property %s.%s of type %s", entity, propName, propValue.getType());
          type.createProperty(propName, propValue.getType());
        }

        for (String sample : propValue.getContents()) {
          dictionary.getIdByName(sample, true);
        }
      }
    }

    ((SchemaImpl) database.getSchema()).saveConfiguration();

    database.commit();
    database.begin();
  }

  protected void dumpSchema(final AnalyzedSchema schema, final long parsedObjects) {
    LogManager.instance().info(this, "---------------------------------------------------------------");
    LogManager.instance().info(this, "Objects found %d", parsedObjects);
    for (String entity : schema.getEntities()) {
      LogManager.instance().info(this, "---------------------------------------------------------------");
      LogManager.instance().info(this, "Entity '%s':", entity);

      for (Map.Entry<String, AnalyzedProperty> p : schema.getProperties(entity)) {
        LogManager.instance().info(this, "- %s (%s)", p.getKey(), p.getValue().getType());
        if (p.getValue().isCollectingSamples())
          LogManager.instance().info(this, "    contents (%d items): %s", p.getValue().getContents().size(), p.getValue().getContents());
      }
    }
    LogManager.instance().info(this, "---------------------------------------------------------------");
  }

  protected void parseParameter(final String name, final String value) {
    if ("-url".equals(name))
      url = value;
    else if ("-database".equals(name))
      databaseURL = value;
    else if ("-forceDatabaseCreate".equals(name))
      forceDatabaseCreate = Boolean.parseBoolean(value);
    else if ("-commitEvery".equals(name))
      commitEvery = Integer.parseInt(value);
    else if ("-parallel".equals(name))
      parallel = Integer.parseInt(value);
    else if ("-vertexType".equals(name))
      vertexTypeName = value;
    else if ("-edgeType".equals(name))
      edgeTypeName = value;
    else if ("-id".equals(name))
      typeIdProperty = value;
    else if ("-idUnique".equals(name))
      typeIdPropertyIsUnique = Boolean.parseBoolean(value);
    else if ("-idType".equals(name))
      typeIdType = value;
    else
      throw new IllegalArgumentException("Invalid setting '" + name + "'");
  }
}
