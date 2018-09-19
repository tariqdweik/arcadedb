/*
 * Copyright (c) 2018 - Arcade Analytics LTD (https://arcadeanalytics.com)
 */

package com.arcadedb.importer;

import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.engine.Dictionary;
import com.arcadedb.schema.*;
import com.arcadedb.utility.LogManager;

import java.util.Map;
import java.util.Timer;
import java.util.TimerTask;

public class Importer {
  protected Database database;
  protected Source   source;
  protected Timer    timer;

  // SETTINGS
  protected Parser           parser;
  protected ImporterSettings settings = new ImporterSettings();
  protected ImporterContext  context  = new ImporterContext();

  protected enum RECORD_TYPE {DOCUMENT, VERTEX}

  public Importer(final String[] args) {
    parseParameters(args);
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
        LogManager.instance().warn(this, "XML importing aborted because unable to determine the schema");
        return;
      }

      openDatabase();

      updateDatabaseSchema(sourceSchema.getSchema());

      source = sourceDiscovery.getSource();
      parser = new Parser(source, 0);
      parser.reset();

      startImporting();

      sourceSchema.getContentImporter().load(parser, database, context, settings);

    } catch (Exception e) {
      LogManager.instance().error(this, "Error on parsing source %s", e, source);
    } finally {
      if (database != null) {
        database.asynch().waitCompletion();
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
      LogManager.instance().info(this, "Parsed %d (%d/sec) - %d documents (%d/sec) - %d vertices (%d/sec) - %d edges (%d/sec)", context.parsed,
          ((context.parsed - context.lastParsed) / deltaInSecs), context.createdDocuments, (context.createdDocuments - context.lastDocuments) / deltaInSecs,
          context.createdVertices, (context.createdVertices - context.lastVertices) / deltaInSecs, context.createdEdges,
          (context.createdEdges - context.lastEdges) / deltaInSecs);
    } else {
      final int progressPerc = (int) (parser.getPosition() * 100 / source.totalSize);
      LogManager.instance().info(this, "Parsed %d (%d/sec - %d%%) - %d records (%d/sec) - %d vertices (%d/sec) - %d edges (%d/sec)", context.parsed,
          ((context.parsed - context.lastParsed) / deltaInSecs), progressPerc, context.createdDocuments,
          (context.createdDocuments - context.lastDocuments) / deltaInSecs, context.createdVertices,
          (context.createdVertices - context.lastVertices) / deltaInSecs, context.createdEdges, (context.createdEdges - context.lastEdges) / deltaInSecs);
    }
    context.lastLapOn = System.currentTimeMillis();
    context.lastParsed = context.parsed;

    context.lastDocuments = context.createdDocuments;
    context.lastVertices = context.createdVertices;
    context.lastEdges = context.createdEdges;
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

    database = factory.exists() ? factory.open() : factory.create();

    database.begin();
    settings.vertexTypeName = getOrCreateVertexType(settings.vertexTypeName).getName();
    settings.edgeTypeName = getOrCreateEdgeType(settings.edgeTypeName).getName();
    database.commit();

    database.setReadYourWrites(false);
    database.asynch().setParallelLevel(settings.parallel);
    database.asynch().setCommitEvery(settings.commitEvery);

    database.begin();
  }

  protected void parseParameters(final String[] args) {
    if (args != null)
      for (int i = 0; i < args.length - 1; i += 2)
        settings.parseParameter(args[i].substring(1), args[i + 1]);
  }

  protected void beginTxIfNeeded() {
    if (!database.isTransactionActive())
      database.begin();
  }

  protected DocumentType getOrCreateDocumentType(final String name) {
    if (!database.getSchema().existsType(name)) {
      LogManager.instance().info(this, "Creating type '%s' of type DOCUMENT", name);

      beginTxIfNeeded();
      final DocumentType type = database.getSchema().createDocumentType(name, settings.parallel);
      if (settings.typeIdProperty != null) {
        type.createProperty(settings.typeIdProperty, Type.getTypeByName(settings.typeIdType));
        database.getSchema()
            .createClassIndexes(SchemaImpl.INDEX_TYPE.LSM_TREE, settings.typeIdPropertyIsUnique, name, new String[] { settings.typeIdProperty });
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
      final VertexType type = database.getSchema().createVertexType(realName, settings.parallel);
      if (settings.typeIdProperty != null) {
        type.createProperty(settings.typeIdProperty, Type.getTypeByName(settings.typeIdType));
        database.getSchema()
            .createClassIndexes(SchemaImpl.INDEX_TYPE.LSM_TREE, settings.typeIdPropertyIsUnique, realName, new String[] { settings.typeIdProperty });
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
      return database.getSchema().createEdgeType(realName, settings.parallel);
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
}
