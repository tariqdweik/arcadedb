/*
 * Copyright (c) 2018 - Arcade Analytics LTD (https://arcadeanalytics.com)
 */

package com.arcadedb.importer;

import com.arcadedb.database.*;
import com.arcadedb.database.async.CreateOutgoingEdgesAsyncTask;
import com.arcadedb.database.async.NewEdgeCallback;
import com.arcadedb.database.async.NewRecordCallback;
import com.arcadedb.graph.Edge;
import com.arcadedb.graph.MutableVertex;
import com.arcadedb.graph.Vertex;
import com.arcadedb.graph.VertexInternal;
import com.arcadedb.index.CompressedAny2RIDIndex;
import com.arcadedb.log.LogManager;
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
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;
import java.util.logging.Level;

public class CSVImporter extends AbstractContentImporter {
  private static final Object[]       NO_PARAMS = new Object[] {};
  private              Object         lastSourceKey;
  private              VertexInternal lastSourceVertex;

  @Override
  public void load(final SourceSchema sourceSchema, final AnalyzedEntity.ENTITY_TYPE entityType, final Parser parser,
      final DatabaseInternal database, final ImporterContext context, final ImporterSettings settings,
      final CompressedAny2RIDIndex<Long> inMemoryIndex) throws ImportException {

    context.parsed.set(0);

    switch (entityType) {
    case DOCUMENT:
      loadDocuments(sourceSchema, parser, database, context, settings, inMemoryIndex);
      break;

    case VERTEX:
      loadVertices(sourceSchema, parser, database, context, settings, inMemoryIndex);
      break;

    case EDGE:
      loadEdges(sourceSchema, parser, database, context, settings, inMemoryIndex);
      break;
    }
  }

  private void loadDocuments(final SourceSchema sourceSchema, final Parser parser, final Database database, final ImporterContext context,
      final ImporterSettings settings, CompressedAny2RIDIndex<Long> inMemoryIndex) throws ImportException {
    AbstractParser csvParser = createCSVParser(settings, ",");

    LogManager.instance().log(this, Level.INFO, "Started importing documents from CSV source", null);

    final long beginTime = System.currentTimeMillis();

    try (final InputStreamReader inputFileReader = new InputStreamReader(parser.getInputStream());) {
      csvParser.beginParsing(inputFileReader);

      if (!database.isTransactionActive())
        database.begin();

      String[] row;
      for (long line = 0; (row = csvParser.parseNext()) != null; ++line) {
        context.parsed.incrementAndGet();

        if (settings.skipEntries > 0 && line < settings.skipEntries)
          // SKIP IT
          continue;

        final MutableDocument document = database.newDocument(settings.documentTypeName);
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
      final ImporterSettings settings, final CompressedAny2RIDIndex<Long> inMemoryIndex) throws ImportException {
    AbstractParser csvParser = createCSVParser(settings, ",");

    LogManager.instance().log(this, Level.INFO, "Started importing vertices from CSV source", null);

    final long beginTime = System.currentTimeMillis();

    try (final InputStreamReader inputFileReader = new InputStreamReader(parser.getInputStream());) {
      csvParser.beginParsing(inputFileReader);

      final AnalyzedProperty id = sourceSchema.getSchema().getEntity(settings.vertexTypeName).getProperty(settings.typeIdProperty);

      if (id == null) {
        LogManager.instance().log(this, Level.INFO, "Property Id '%s.%s' is null. Importing is aborted", null, settings.vertexTypeName,
            settings.typeIdProperty);
        throw new IllegalArgumentException(
            "Property Id '" + settings.vertexTypeName + "." + settings.typeIdProperty + "' is null. Importing is aborted");
      }

      if (!database.isTransactionActive())
        database.begin();

      String[] row;
      for (long line = 0; (row = csvParser.parseNext()) != null; ++line) {
        context.parsed.incrementAndGet();

        if (settings.skipEntries > 0 && line < settings.skipEntries)
          // SKIP IT
          continue;

        final int idIndex = id.getIndex();
        if (idIndex >= row.length) {
          LogManager.instance()
              .log(this, Level.INFO, "Property Id is configured on property %d but cannot be found on current record. Skip it", null,
                  idIndex);
          continue;
        }

        final long vertexId = Long.parseLong(row[idIndex]);

        final Vertex sourceVertex;
        RID sourceVertexRID = inMemoryIndex.get(vertexId);
        if (sourceVertexRID == null) {
          // CREATE THE VERTEX
          sourceVertex = database.newVertex(settings.vertexTypeName);
          ((MutableVertex) sourceVertex).set(settings.typeIdProperty, vertexId);
          database.async().createRecord((MutableDocument) sourceVertex, new NewRecordCallback() {
            @Override
            public void call(final Record newDocument) {
              final AtomicReference<VertexInternal> v = new AtomicReference<>((VertexInternal) sourceVertex);
              // PRE-CREATE OUT/IN CHUNKS TO SPEEDUP EDGE CREATION
              final DatabaseInternal db = (DatabaseInternal) database;
              db.getGraphEngine().createOutEdgeChunk(db, v);
              db.getGraphEngine().createInEdgeChunk(db, v);

              context.createdVertices.incrementAndGet();
              inMemoryIndex.put(vertexId, newDocument.getIdentity());
            }
          });
        }

        if (line > 0 && line % 1000000 == 0)
          LogManager.instance().log(this, Level.INFO, "Map chunkSize=%s chunkAllocated=%s size=%d totalUsedSlots=%d", null,
              FileUtils.getSizeAsString(inMemoryIndex.getChunkSize()), FileUtils.getSizeAsString(inMemoryIndex.getChunkAllocated()),
              inMemoryIndex.size(), inMemoryIndex.getTotalUsedSlots());
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
      final ImporterContext context, final ImporterSettings settings, CompressedAny2RIDIndex<Long> verticesIndex) throws ImportException {
    AbstractParser csvParser = createCSVParser(settings, ",");

    LogManager.instance().log(this, Level.INFO, "Started importing edges from CSV source", null);

    final long beginTime = System.currentTimeMillis();

    if (verticesIndex == null || verticesIndex.isEmpty())
      LogManager.instance()
          .log(this, Level.WARNING, "Cannot find in-memory index for vertices, loading from disk could be very slow", null);

//    final CompressedRID2RIDsIndex edgeIndex = new CompressedRID2RIDsIndex(database, 50 * 1024 * 1024);

    List<Pair<Identifiable, Object[]>> connections = new ArrayList<>();

    try (final InputStreamReader inputFileReader = new InputStreamReader(parser.getInputStream());) {
      csvParser.beginParsing(inputFileReader);

      final AnalyzedProperty from = sourceSchema.getSchema().getEntity(settings.edgeTypeName).getProperty(settings.edgeFromField);
      final AnalyzedProperty to = sourceSchema.getSchema().getEntity(settings.edgeTypeName).getProperty(settings.edgeToField);

      if (!database.isTransactionActive())
        database.begin();

      long edgeLines = 0;
      String[] row;
      for (long line = 0; (row = csvParser.parseNext()) != null; ++line) {
        context.parsed.incrementAndGet();

        if (settings.skipEntries > 0 && line < settings.skipEntries)
          // SKIP IT
          continue;

        final long destinationVertexKey = Long.parseLong(row[to.getIndex()]);
        final RID destinationVertexRID = verticesIndex.get(destinationVertexKey);
        if (destinationVertexRID == null) {
          // SKIP IT
          context.skippedEdges.incrementAndGet();
          continue;
        }

        final long sourceVertexKey = Long.parseLong(row[from.getIndex()]);

        if (lastSourceKey == null || !lastSourceKey.equals(sourceVertexKey)) {
          createEdgesInBatch(database, context, settings, connections);
          connections = new ArrayList<>();

          final RID sourceVertexRID = verticesIndex.get(sourceVertexKey);
          if (sourceVertexRID == null) {
            // SKIP IT
            context.skippedEdges.incrementAndGet();
            continue;
          }

          lastSourceKey = sourceVertexKey;
          lastSourceVertex = (VertexInternal) sourceVertexRID.getVertex(true);
        }

        connections.add(new Pair<>(destinationVertexRID, NO_PARAMS));

        ++edgeLines;

        if (edgeLines % settings.commitEvery == 0) {
          createEdgesInBatch(database, context, settings, connections);
          connections = new ArrayList<>();
          database.commit();
          database.begin();
        }
      }

      createEdgesInBatch(database, context, settings, connections);

      database.commit();
      database.async().waitCompletion();

      // CREATE INCOMING CONNECTIONS
      database.begin();
      database.getGraphEngine().createIncomingConnectionsInBatch(database, settings.vertexTypeName, settings.edgeTypeName);
      database.commit();

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

  private void createEdgesInBatch(final DatabaseInternal database, final ImporterContext context, final ImporterSettings settings,
      List<Pair<Identifiable, Object[]>> connections) {
    if (!connections.isEmpty()) {
      // CREATE EDGES ALL TOGETHER FOR THE PREVIOUS BATCH
      if (lastSourceVertex.getOutEdgesHeadChunk() == null)
        // RELOAD IT
        lastSourceVertex = (VertexInternal) lastSourceVertex.getIdentity().getVertex();

      final int asyncSlot = database.async().getSlot(lastSourceVertex.getIdentity().getBucketId());

      database.async().scheduleTask(asyncSlot,
          new CreateOutgoingEdgesAsyncTask(lastSourceVertex, connections, settings.edgeTypeName, settings.edgeBidirectional,
              new NewEdgeCallback() {
                @Override
                public void call(Edge newEdge, boolean createdSourceVertex, boolean createdDestinationVertex) {
                  context.createdEdges.addAndGet(connections.size());
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
        } else
          // DATA LINE
          for (int i = 0; i < row.length; ++i)
            analyzedSchema.getOrCreateEntity(entityName, entityType).getOrCreateProperty(fieldNames.get(i), row[i]);

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
