/*
 * Copyright (c) 2018 - Arcade Analytics LTD (https://arcadeanalytics.com)
 */

package com.arcadedb.importer;

import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.engine.Dictionary;
import com.arcadedb.log.LogManager;
import com.arcadedb.schema.*;

import java.util.Timer;
import java.util.TimerTask;
import java.util.logging.Level;

public class Importer {
  protected Database database;
  protected Source   source;
  protected Timer    timer;

  // SETTINGS
  protected Parser           parser;
  protected ImporterSettings settings = new ImporterSettings();
  protected ImporterContext  context  = new ImporterContext();

  protected enum RECORD_TYPE {DOCUMENT, VERTEX, EDGE}

  public Importer(final String[] args) {
    settings.parseParameters(args);
  }

  public static void main(final String[] args) {
    new Importer(args).load();
  }

  public void load() {
    Source source = null;

    try {
      final SourceDiscovery sourceDiscovery = new SourceDiscovery(settings.url);

      final SourceSchema sourceSchema = sourceDiscovery.getSchema(settings);
      if (sourceSchema == null) {
        LogManager.instance().log(this, Level.WARNING, "XML importing aborted because unable to determine the schema");
        return;
      }

      openDatabase();

      updateDatabaseSchema(sourceSchema.getSchema());

      source = sourceDiscovery.getSource();
      parser = new Parser(source, 0);
      parser.reset();

      startImporting();

      sourceSchema.getContentImporter().load(sourceSchema, parser, database, context, settings);

    } catch (Exception e) {
      LogManager.instance().log(this, Level.SEVERE, "Error on parsing source %s", e, source);
    } finally {
      if (database != null) {
        database.async().waitCompletion();
        stopImporting();
        closeDatabase();
      }
      closeInputFile();
    }
  }

  protected void printProgress() {
    long deltaInSecs = (System.currentTimeMillis() - context.lastLapOn) / 1000;
    if (deltaInSecs == 0)
      deltaInSecs = 1;

    if (source == null || source.totalSize < 0) {
      LogManager.instance()
          .log(this, Level.INFO, "Parsed %d (%d/sec) - %d documents (%d/sec) - %d vertices (%d/sec) - %d edges (%d/sec)", null, context.parsed.get(),
              ((context.parsed.get() - context.lastParsed) / deltaInSecs), context.createdDocuments.get(),
              (context.createdDocuments.get() - context.lastDocuments) / deltaInSecs, context.createdVertices.get(),
              (context.createdVertices.get() - context.lastVertices) / deltaInSecs, context.createdEdges.get(),
              (context.createdEdges.get() - context.lastEdges) / deltaInSecs);
    } else {
      final int progressPerc = (int) (parser.getPosition() * 100 / source.totalSize);
      LogManager.instance()
          .log(this, Level.INFO, "Parsed %d (%d/sec - %d%%) - %d records (%d/sec) - %d vertices (%d/sec) - %d edges (%d/sec)", null, context.parsed.get(),
              ((context.parsed.get() - context.lastParsed) / deltaInSecs), progressPerc, context.createdDocuments.get(),
              (context.createdDocuments.get() - context.lastDocuments) / deltaInSecs, context.createdVertices.get(),
              (context.createdVertices.get() - context.lastVertices) / deltaInSecs, context.createdEdges.get(),
              (context.createdEdges.get() - context.lastEdges) / deltaInSecs);
    }
    context.lastLapOn = System.currentTimeMillis();
    context.lastParsed = context.parsed.get();

    context.lastDocuments = context.createdDocuments.get();
    context.lastVertices = context.createdVertices.get();
    context.lastEdges = context.createdEdges.get();
  }

  protected void startImporting() {
    context.startedOn = context.lastLapOn = System.currentTimeMillis();

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

    final DatabaseFactory factory = new DatabaseFactory(settings.databaseURL);

    if (settings.forceDatabaseCreate) {
      if (factory.exists())
        factory.open().drop();
    }

    if (factory.exists()) {
      LogManager.instance().log(this, Level.INFO, "Opening database '%s'...", null, settings.databaseURL);
      database = factory.open();
    } else {
      LogManager.instance().log(this, Level.INFO, "Creating database '%s'...", null, settings.databaseURL);
      database = factory.create();
    }

    database.begin();
    if (settings.recordType == RECORD_TYPE.VERTEX || settings.recordType == RECORD_TYPE.EDGE) {
      settings.vertexTypeName = getOrCreateVertexType(settings.vertexTypeName).getName();
      settings.edgeTypeName = getOrCreateEdgeType(settings.edgeTypeName).getName();
    } else
      settings.documentTypeName = getOrCreateDocumentType(settings.documentTypeName).getName();

    database.commit();

    database.setReadYourWrites(false);
    database.async().setParallelLevel(settings.parallel);
    database.async().setCommitEvery(settings.commitEvery);

    database.begin();
  }

  protected void beginTxIfNeeded() {
    if (!database.isTransactionActive())
      database.begin();
  }

  protected DocumentType getOrCreateDocumentType(final String name) {
    if (!database.getSchema().existsType(name)) {
      LogManager.instance().log(this, Level.INFO, "Creating type '%s' of type DOCUMENT", null, name);

      beginTxIfNeeded();
      final DocumentType type = database.getSchema().createDocumentType(name, settings.parallel);
      if (settings.typeIdProperty != null) {
        type.createProperty(settings.typeIdProperty, Type.getTypeByName(settings.typeIdType));
        database.getSchema().createIndexes(SchemaImpl.INDEX_TYPE.LSM_TREE, settings.typeIdPropertyIsUnique, name, new String[] { settings.typeIdProperty });
      }
      return type;
    }
    return database.getSchema().getType(name);
  }

  protected VertexType getOrCreateVertexType(final String name) {
    if (!database.getSchema().existsType(name)) {
      LogManager.instance().log(this, Level.INFO, "Creating type '%s' of type VERTEX", null, name);

      beginTxIfNeeded();
      final VertexType type = database.getSchema().createVertexType(name, settings.parallel);
      if (settings.typeIdProperty != null) {
        type.createProperty(settings.typeIdProperty, Type.getTypeByName(settings.typeIdType));
        database.getSchema().createIndexes(SchemaImpl.INDEX_TYPE.LSM_TREE, settings.typeIdPropertyIsUnique, name, new String[] { settings.typeIdProperty });
      }
      return type;
    }

    return (VertexType) database.getSchema().getType(name);
  }

  protected EdgeType getOrCreateEdgeType(final String name) {
    if (!database.getSchema().existsType(name)) {
      LogManager.instance().log(this, Level.INFO, "Creating type '%s' of type EDGE", null, name);

      beginTxIfNeeded();
      return database.getSchema().createEdgeType(name, settings.parallel);
    }

    return (EdgeType) database.getSchema().getType(name);
  }

  protected void updateDatabaseSchema(final AnalyzedSchema schema) {
    if (schema == null)
      return;

    final Dictionary dictionary = database.getSchema().getDictionary();

    LogManager.instance().log(this, Level.INFO, "Checking schema...");

    for (String entity : schema.getEntities()) {
      final DocumentType type;
      switch (settings.recordType) {
      case VERTEX:
        type = getOrCreateVertexType(entity);
        break;
      case EDGE:
        type = getOrCreateEdgeType(entity);
        break;
      case DOCUMENT:
        type = getOrCreateDocumentType(entity);
        break;
      default:
        throw new IllegalArgumentException("Record type '" + settings.recordType + "' is not supported");
      }

      for (AnalyzedProperty propValue : schema.getProperties(entity)) {
        final String propName = propValue.getName();

        if (type.existsProperty(propName)) {
          // CHECK TYPE
          final Property property = type.getProperty(propName);
          if (property.getType() != propValue.getType()) {
            LogManager.instance()
                .log(this, Level.WARNING, "- found schema property %s.%s of type %s, while analyzing the source type %s was found", null, entity, propName,
                    property.getType(), propValue.getType());
          }
        } else {
          // CREATE IT
          LogManager.instance().log(this, Level.INFO, "- creating property %s.%s of type %s", null, entity, propName, propValue.getType());
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
    LogManager.instance().log(this, Level.INFO, "---------------------------------------------------------------");
    LogManager.instance().log(this, Level.INFO, "Objects found %d", null, parsedObjects);
    for (String entity : schema.getEntities()) {
      LogManager.instance().log(this, Level.INFO, "---------------------------------------------------------------");
      LogManager.instance().log(this, Level.INFO, "Entity '%s':", null, entity);

      for (AnalyzedProperty p : schema.getProperties(entity)) {
        LogManager.instance().log(this, Level.INFO, "- %s (%s)", null, p.getName(), p.getType());
        if (p.isCollectingSamples())
          LogManager.instance().log(this, Level.INFO, "    contents (%d items): %s", null, p.getContents().size(), p.getContents());
      }
    }
    LogManager.instance().log(this, Level.INFO, "---------------------------------------------------------------");
  }
}
