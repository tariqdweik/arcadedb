/*
 * Copyright (c) 2018 - Arcade Analytics LTD (https://arcadeanalytics.com)
 */

package com.arcadedb.importer;

import com.arcadedb.database.*;
import com.arcadedb.database.async.CreateOutgoingEdgesAsyncTask;
import com.arcadedb.database.async.NewEdgesCallback;
import com.arcadedb.database.async.NewRecordCallback;
import com.arcadedb.graph.Edge;
import com.arcadedb.graph.MutableVertex;
import com.arcadedb.graph.Vertex;
import com.arcadedb.graph.VertexInternal;
import com.arcadedb.index.CompressedAny2RIDIndex;
import com.arcadedb.index.CompressedRID2RIDsIndex;
import com.arcadedb.log.LogManager;
import com.arcadedb.schema.Type;
import com.arcadedb.utility.FileUtils;
import com.arcadedb.utility.Pair;
import com.univocity.parsers.common.AbstractParser;
import com.univocity.parsers.csv.CsvParser;
import com.univocity.parsers.csv.CsvParserSettings;
import com.univocity.parsers.tsv.TsvParser;
import com.univocity.parsers.tsv.TsvParserSettings;

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import java.util.logging.Level;

public class CSVImporter extends AbstractContentImporter {
  private static final Object[]       NO_PARAMS = new Object[] {};
  public static final  int            _16MB     = 16 * 1024 * 1024;
  private              Object         lastSourceKey;
  private              VertexInternal lastSourceVertex;

  @Override
  public void load(final SourceSchema sourceSchema, final AnalyzedEntity.ENTITY_TYPE entityType, final Parser parser,
      final DatabaseInternal database, final ImporterContext context, final ImporterSettings settings) throws ImportException {

    context.parsed.set(0);

    switch (entityType) {
    case DOCUMENT:
      loadDocuments(sourceSchema, parser, database, context, settings);
      break;

    case VERTEX:
      loadVertices(sourceSchema, parser, database, context, settings);
      break;

    case EDGE:
      loadEdges(sourceSchema, parser, database, context, settings);
      break;
    }
  }

  private void loadDocuments(final SourceSchema sourceSchema, final Parser parser, final Database database, final ImporterContext context,
      final ImporterSettings settings) throws ImportException {
    AbstractParser csvParser = createCSVParser(settings, ",");

    LogManager.instance().log(this, Level.INFO, "Started importing documents from CSV source", null);

    final long beginTime = System.currentTimeMillis();

    try (final InputStreamReader inputFileReader = new InputStreamReader(parser.getInputStream());) {
      csvParser.beginParsing(inputFileReader);

      if (!database.isTransactionActive())
        database.begin();

      final AnalyzedEntity entity = sourceSchema.getSchema().getEntity(settings.vertexTypeName);

      final List<AnalyzedProperty> properties = new ArrayList<>();
      if (!settings.documentPropertiesInclude.equalsIgnoreCase("*")) {
        final String[] includes = settings.documentPropertiesInclude.split(",");
        final Set<String> propertiesSet = new HashSet<>();

        for (String i : includes)
          propertiesSet.add(i);

        for (AnalyzedProperty p : entity.getProperties()) {
          if (propertiesSet.contains(p.getName())) {
            properties.add(p);
          }
        }
      } else {
        // INCLUDE ALL THE PROPERTIES
        for (AnalyzedProperty p : entity.getProperties())
          properties.add(p);
      }

      LogManager.instance().log(this, Level.INFO, "Importing the following document properties: %s", null, properties);

      String[] row;
      for (long line = 0; (row = csvParser.parseNext()) != null; ++line) {
        context.parsed.incrementAndGet();

        if (settings.skipEntries > 0 && line < settings.skipEntries)
          // SKIP IT
          continue;

        final MutableDocument document = database.newDocument(settings.documentTypeName);

        for (int p = 0; p < properties.size(); ++p) {
          final AnalyzedProperty prop = properties.get(p);
          document.set(prop.getName(), row[prop.getIndex()]);
        }

        database.async().createRecord(document, new NewRecordCallback() {
          @Override
          public void call(final Record newDocument) {
            context.createdDocuments.incrementAndGet();
          }
        });
      }

      database.commit();
      database.async().waitCompletion();

    } catch (IOException e) {
      throw new ImportException("Error on importing CSV");
    } finally {
      final long elapsedInSecs = (System.currentTimeMillis() - beginTime) / 1000;
      LogManager.instance()
          .log(this, Level.INFO, "Importing of documents from CSV source completed in %d seconds (%d/sec)", null, elapsedInSecs,
              elapsedInSecs > 0 ? context.createdDocuments.get() / elapsedInSecs : context.createdDocuments.get());
      LogManager.instance().log(this, Level.INFO, "- Parsed lines...: %d", null, context.parsed.get());
      LogManager.instance().log(this, Level.INFO, "- Total documents: %d", null, context.createdDocuments.get());
    }
  }

  private void loadVertices(final SourceSchema sourceSchema, final Parser parser, final Database database, final ImporterContext context,
      final ImporterSettings settings) throws ImportException {

    final AnalyzedEntity entity = sourceSchema.getSchema().getEntity(settings.vertexTypeName);
    final AnalyzedProperty id = entity.getProperty(settings.typeIdProperty);

    if (context.verticesIndex == null) {
      long expectedVertices = settings.expectedVertices;
      if (expectedVertices <= 0 && entity != null)
        expectedVertices = (int) (sourceSchema.getSource().totalSize / entity.getAverageRowLength());

      if (expectedVertices <= 0)
        expectedVertices = 1000000;
      else if (expectedVertices > Integer.MAX_VALUE)
        expectedVertices = Integer.MAX_VALUE;

      context.verticesIndex = new CompressedAny2RIDIndex<>(database, Type.LONG, (int) expectedVertices);
    }

    final AbstractParser csvParser = createCSVParser(settings, ",");

    LogManager.instance().log(this, Level.INFO, "Started importing vertices from CSV source", null);

    final long beginTime = System.currentTimeMillis();

    try (final InputStreamReader inputFileReader = new InputStreamReader(parser.getInputStream());) {
      csvParser.beginParsing(inputFileReader);

      if (id == null) {
        LogManager.instance().log(this, Level.INFO, "Property Id '%s.%s' is null. Importing is aborted", null, settings.vertexTypeName,
            settings.typeIdProperty);
        throw new IllegalArgumentException(
            "Property Id '" + settings.vertexTypeName + "." + settings.typeIdProperty + "' is null. Importing is aborted");
      }
      final int idIndex = id.getIndex();

      final List<AnalyzedProperty> properties = new ArrayList<>();
      if (!settings.vertexPropertiesInclude.equalsIgnoreCase("*")) {
        final String[] includes = settings.vertexPropertiesInclude.split(",");
        final Set<String> propertiesSet = new HashSet<>();

        for (String i : includes)
          propertiesSet.add(i);

        for (AnalyzedProperty p : entity.getProperties()) {
          if (propertiesSet.contains(p.getName())) {
            properties.add(p);
          }
        }
      } else {
        // INCLUDE ALL THE PROPERTIES
        for (AnalyzedProperty p : entity.getProperties())
          properties.add(p);
      }

      LogManager.instance().log(this, Level.INFO, "Importing the following vertex properties: %s", null, properties);

      if (!database.isTransactionActive())
        database.begin();

      String[] row;
      for (long line = 0; (row = csvParser.parseNext()) != null; ++line) {
        context.parsed.incrementAndGet();

        if (settings.skipEntries > 0 && line < settings.skipEntries)
          // SKIP IT
          continue;

        if (idIndex >= row.length) {
          LogManager.instance()
              .log(this, Level.INFO, "Property Id is configured on property %d but cannot be found on current record. Skip it", null,
                  idIndex);
          continue;
        }

        final long vertexId = Long.parseLong(row[idIndex]);

        final Vertex sourceVertex;
        RID sourceVertexRID = context.verticesIndex.get(vertexId);
        if (sourceVertexRID == null) {
          // CREATE THE VERTEX
          sourceVertex = database.newVertex(settings.vertexTypeName);

          for (int p = 0; p < properties.size(); ++p) {
            final AnalyzedProperty prop = properties.get(p);
            ((MutableVertex) sourceVertex).set(prop.getName(), row[prop.getIndex()]);
          }

          database.async().createRecord((MutableDocument) sourceVertex, new NewRecordCallback() {
            @Override
            public void call(final Record newDocument) {
              final AtomicReference<VertexInternal> v = new AtomicReference<>((VertexInternal) sourceVertex);
              // PRE-CREATE OUT/IN CHUNKS TO SPEEDUP EDGE CREATION
              final DatabaseInternal db = (DatabaseInternal) database;
              db.getGraphEngine().createOutEdgeChunk(db, v);
              db.getGraphEngine().createInEdgeChunk(db, v);

              context.createdVertices.incrementAndGet();
              context.verticesIndex.put(vertexId, newDocument.getIdentity());
            }
          });
        }

        if (line > 0 && line % 10000000 == 0)
          LogManager.instance().log(this, Level.INFO, "Map chunkSize=%s chunkAllocated=%s size=%d totalUsedSlots=%d", null,
              FileUtils.getSizeAsString(context.verticesIndex.getChunkSize()),
              FileUtils.getSizeAsString(context.verticesIndex.getChunkAllocated()), context.verticesIndex.size(),
              context.verticesIndex.getTotalUsedSlots());
      }

      database.commit();
      database.async().waitCompletion();

    } catch (IOException e) {
      throw new ImportException("Error on importing CSV");
    } finally {
      final long elapsedInSecs = (System.currentTimeMillis() - beginTime) / 1000;
      LogManager.instance()
          .log(this, Level.INFO, "Importing of vertices from CSV source completed in %d seconds (%d/sec)", null, elapsedInSecs,
              elapsedInSecs > 0 ? context.createdVertices.get() / elapsedInSecs : context.createdVertices.get());
      LogManager.instance().log(this, Level.INFO, "- Parsed lines...: %d", null, context.parsed.get());
      LogManager.instance().log(this, Level.INFO, "- Total vertices.: %d", null, context.createdVertices.get());
    }

  }

  private void loadEdges(final SourceSchema sourceSchema, final Parser parser, final DatabaseInternal database,
      final ImporterContext context, final ImporterSettings settings) throws ImportException {
    AbstractParser csvParser = createCSVParser(settings, ",");

    LogManager.instance().log(this, Level.INFO, "Started importing edges from CSV source", null);

    final long beginTime = System.currentTimeMillis();

    if (context.verticesIndex == null || context.verticesIndex.isEmpty())
      LogManager.instance()
          .log(this, Level.WARNING, "Cannot find in-memory index for vertices, loading from disk could be very slow", null);

    final AnalyzedEntity entity = sourceSchema.getSchema().getEntity(settings.edgeTypeName);
    final AnalyzedProperty from = entity.getProperty(settings.edgeFromField);
    final AnalyzedProperty to = entity.getProperty(settings.edgeToField);

    long expectedEdges = settings.expectedEdges;
    if (expectedEdges <= 0 && entity != null)
      expectedEdges = (int) (sourceSchema.getSource().totalSize / entity.getAverageRowLength());

    if (expectedEdges <= 0 || expectedEdges > _16MB)
      // USE CHUNKS OF 16MB EACH
      expectedEdges = _16MB;

    final CompressedRID2RIDsIndex incomingConnectionsIndex = new CompressedRID2RIDsIndex(database, (int) expectedEdges);

    try (final InputStreamReader inputFileReader = new InputStreamReader(parser.getInputStream());) {
      csvParser.beginParsing(inputFileReader);

      final List<AnalyzedProperty> properties = new ArrayList<>();
      if (!settings.edgePropertiesInclude.equalsIgnoreCase("*")) {
        final String[] includes = settings.edgePropertiesInclude.split(",");
        final Set<String> propertiesSet = new HashSet<>();

        for (String i : includes)
          propertiesSet.add(i);

        for (AnalyzedProperty p : entity.getProperties()) {
          if (propertiesSet.contains(p.getName())) {
            properties.add(p);
          }
        }
      } else {
        // INCLUDE ALL THE PROPERTIES
        for (AnalyzedProperty p : entity.getProperties())
          properties.add(p);
      }

      LogManager.instance().log(this, Level.INFO, "Importing the following edge properties: %s", null, properties);

      if (!database.isTransactionActive())
        database.begin();

      List<Pair<Identifiable, Object[]>> connections = new ArrayList<>();

      long edgeLines = 0;
      String[] row;
      for (long line = 0; (row = csvParser.parseNext()) != null; ++line) {
        context.parsed.incrementAndGet();

        if (settings.skipEntries > 0 && line < settings.skipEntries)
          // SKIP IT
          continue;

        final long destinationVertexKey = Long.parseLong(row[to.getIndex()]);
        // TODO: LOAD FROM INDEX
        final RID destinationVertexRID = context.verticesIndex.get(destinationVertexKey);
        if (destinationVertexRID == null) {
          // SKIP IT
          context.skippedEdges.incrementAndGet();
          continue;
        }

        final long sourceVertexKey = Long.parseLong(row[from.getIndex()]);

        if (lastSourceKey == null || !lastSourceKey.equals(sourceVertexKey)) {
          createEdgesInBatch(database, incomingConnectionsIndex, context, settings, connections);
          connections = new ArrayList<>();

          // TODO: LOAD FROM INDEX
          final RID sourceVertexRID = context.verticesIndex.get(sourceVertexKey);
          if (sourceVertexRID == null) {
            // SKIP IT
            context.skippedEdges.incrementAndGet();
            continue;
          }

          lastSourceKey = sourceVertexKey;
          lastSourceVertex = (VertexInternal) sourceVertexRID.getVertex(true);
        }

        final Object[] params;
        if (row.length > 2) {
          params = new Object[row.length * 2];
          for (int i = 0; i < row.length; ++i) {
            final AnalyzedProperty property = properties.get(i);
            params[i * 2] = property.getName();
            params[i * 2 + 1] = row[property.getIndex()];
          }
        } else {
          params = NO_PARAMS;
        }

        connections.add(new Pair<>(destinationVertexRID, params));

        ++edgeLines;

        if (incomingConnectionsIndex.getChunkSize() >= settings.maxRAMIncomingEdges) {
          LogManager.instance()
              .log(this, Level.INFO, "Creation of back connections, reached %s size (max=%d), flushing %d connections (resetCounter=%d)...",
                  null, FileUtils.getSizeAsString(incomingConnectionsIndex.getChunkSize()),
                  FileUtils.getSizeAsString(settings.maxRAMIncomingEdges), incomingConnectionsIndex.size(),
                  incomingConnectionsIndex.getResetCounter());

          // CREATE A NEW CHUNK BEFORE CONTINUING
          final CompressedRID2RIDsIndex oldEdgeIndex = new CompressedRID2RIDsIndex(database, incomingConnectionsIndex.reset());

          database.getGraphEngine().createIncomingEdgesInBatch(database, oldEdgeIndex, settings.edgeTypeName);

          LogManager.instance().log(this, Level.INFO, "Creation done, reset index buffer and continue", null);
        }

        if (edgeLines % settings.commitEvery == 0) {
          createEdgesInBatch(database, incomingConnectionsIndex, context, settings, connections);
          connections = new ArrayList<>();
          database.commit();
          database.begin();
        }
      }

      createEdgesInBatch(database, incomingConnectionsIndex, context, settings, connections);

      // FLUSH LAST INCOMING CONNECTIONS
      final CompressedRID2RIDsIndex oldEdgeIndex = new CompressedRID2RIDsIndex(database, incomingConnectionsIndex.reset());
      database.getGraphEngine().createIncomingEdgesInBatch(database, oldEdgeIndex, settings.edgeTypeName);
      LogManager.instance().log(this, Level.INFO, "Creation done, reset index buffer and continue", null);

      database.commit();
      database.async().waitCompletion();

    } catch (IOException e) {
      throw new ImportException("Error on importing CSV");
    } finally {
      final long elapsedInSecs = (System.currentTimeMillis() - beginTime) / 1000;
      try {
        Thread.sleep(300);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
      LogManager.instance()
          .log(this, Level.INFO, "Importing of edges from CSV source completed in %d seconds (%d/sec)", null, elapsedInSecs,
              elapsedInSecs > 0 ? context.createdEdges.get() / elapsedInSecs : context.createdEdges.get());
      LogManager.instance().log(this, Level.INFO, "- Parsed lines...: %d", null, context.parsed.get());
      LogManager.instance().log(this, Level.INFO, "- Total edges....: %d", null, context.createdEdges.get());
      LogManager.instance().log(this, Level.INFO, "- Skipped edges..: %d", null, context.skippedEdges.get());
    }
  }

  private void createEdgesInBatch(final DatabaseInternal database, final CompressedRID2RIDsIndex edgeIndex, final ImporterContext context,
      final ImporterSettings settings, final List<Pair<Identifiable, Object[]>> connections) {
    if (!connections.isEmpty()) {
      // CREATE EDGES ALL TOGETHER FOR THE PREVIOUS BATCH
      if (lastSourceVertex.getOutEdgesHeadChunk() == null)
        // RELOAD IT
        lastSourceVertex = (VertexInternal) lastSourceVertex.getIdentity().getVertex();

      final int asyncSlot = database.async().getSlot(lastSourceVertex.getIdentity().getBucketId());

      database.async().scheduleTask(asyncSlot,
          new CreateOutgoingEdgesAsyncTask(lastSourceVertex, connections, settings.edgeTypeName, false, new NewEdgesCallback() {
            @Override
            public void call(List<Edge> newEdges) {
              context.createdEdges.addAndGet(connections.size());
              for (Edge e : newEdges)
                edgeIndex.put(e.getIn(), e.getIdentity(), lastSourceVertex.getIdentity());
              connections.clear();
            }
          }), true);
    }
  }

  @Override
  public SourceSchema analyze(final AnalyzedEntity.ENTITY_TYPE entityType, final Parser parser, final ImporterSettings settings,
      final AnalyzedSchema analyzedSchema) throws IOException {
    parser.reset();

    String delimiter = ",";

    if (settings.options.containsKey("delimiter"))
      delimiter = settings.options.get("delimiter");

    CsvParserSettings csvParserSettings;
    TsvParserSettings tsvParserSettings;
    AbstractParser csvParser;

    if ("\t".equals(delimiter)) {
      tsvParserSettings = new TsvParserSettings();
      csvParser = new TsvParser(tsvParserSettings);
    } else {
      csvParserSettings = new CsvParserSettings();
      csvParser = new CsvParser(csvParserSettings);
      csvParserSettings.setDelimiterDetectionEnabled(false);
      csvParserSettings.detectFormatAutomatically(delimiter.charAt(0));
      csvParserSettings.getFormat().setDelimiter(delimiter.charAt(0));
    }

    final List<String> fieldNames = new ArrayList<>();

    final String entityName = entityType == AnalyzedEntity.ENTITY_TYPE.DOCUMENT ?
        settings.documentTypeName :
        entityType == AnalyzedEntity.ENTITY_TYPE.VERTEX ? settings.vertexTypeName : settings.edgeTypeName;

    try (final InputStreamReader inputFileReader = new InputStreamReader(parser.getInputStream());) {
      csvParser.beginParsing(inputFileReader);

      String[] row;
      for (long line = 0; (row = csvParser.parseNext()) != null; ++line) {
        if (settings.analysisLimitBytes > 0 && csvParser.getContext().currentChar() > settings.analysisLimitBytes)
          break;

        if (settings.analysisLimitEntries > 0 && line > settings.analysisLimitEntries)
          break;

        if (line == 0) {
          // HEADER
          for (String cell : row)
            fieldNames.add(cell);
        } else {
          // DATA LINE
          final AnalyzedEntity entity = analyzedSchema.getOrCreateEntity(entityName, entityType);

          entity.setRowSize(row);
          for (int i = 0; i < row.length; ++i) {
            entity.getOrCreateProperty(fieldNames.get(i), row[i]);
          }
        }
      }

    } catch (EOFException e) {
      // REACHED THE LIMIT
    } catch (IOException e) {
      throw new ImportException("Error on importing CSV");
    }

    // END OF PARSING. THIS DETERMINES THE TYPE
    analyzedSchema.endParsing();

    return new SourceSchema(this, parser.getSource(), analyzedSchema);
  }

  @Override
  public String getFormat() {
    return "CSV";
  }

  protected AbstractParser createCSVParser(final ImporterSettings settings, String delimiter) {
    if (settings.options.containsKey("delimiter"))
      delimiter = settings.options.get("delimiter");

    if (settings.skipEntries == null)
      // BY DEFAULT SKIP THE FIRST LINE AS HEADER
      settings.skipEntries = 1l;

    CsvParserSettings csvParserSettings;
    TsvParserSettings tsvParserSettings;
    AbstractParser csvParser;

    if ("\t".equals(delimiter)) {
      tsvParserSettings = new TsvParserSettings();
      csvParser = new TsvParser(tsvParserSettings);
    } else {
      csvParserSettings = new CsvParserSettings();
      csvParser = new CsvParser(csvParserSettings);
      csvParserSettings.getFormat().setDelimiter(delimiter.charAt(0));
    }
    return csvParser;
  }
}
