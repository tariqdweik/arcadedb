/*
 * Copyright 2021 Arcade Data Ltd
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package com.arcadedb.importer;

import com.arcadedb.Constants;
import com.arcadedb.database.*;
import com.arcadedb.graph.MutableEdge;
import com.arcadedb.graph.Vertex;
import com.arcadedb.index.Index;
import com.arcadedb.index.lsm.LSMTreeIndexAbstract;
import com.arcadedb.schema.*;
import com.arcadedb.utility.FileUtils;
import com.google.gson.stream.JsonReader;
import com.google.gson.stream.JsonToken;
import org.json.JSONArray;
import org.json.JSONObject;

import java.io.*;
import java.util.*;
import java.util.zip.GZIPInputStream;

import static com.google.gson.stream.JsonToken.*;

/**
 * Importer from OrientDB. OrientDB is a registered mark of SAP.
 *
 * @author Luca Garulli
 */
public class OrientDBImporter {
  private final File                       file;
  private       String                     databasePath;
  private       String                     inputFile;
  private       String                     securityFileName;
  private       String                     databaseName;
  private       boolean                    overwriteDatabase               = false;
  private       Map<String, Integer>       clustersNameToId                = new LinkedHashMap<>();
  private       Map<Integer, String>       clustersIdToName                = new LinkedHashMap<>();
  private       Map<String, OrientDBClass> classes                         = new LinkedHashMap<>();
  private       Map<String, Long>          totalRecordByType               = new HashMap<>();
  private       long                       totalRecordParsed               = 0L;
  private       long                       totalAttributesParsed           = 0L;
  private       long                       errors                          = 0L;
  private       long                       warnings                        = 0L;
  private       Set<String>                excludeClasses                  = new HashSet<>(
      Arrays.asList(new String[] { "OUser", "ORole", "OSchedule", "OSequence", "OTriggered", "OSecurityPolicy", "ORestricted", "OIdentity", "OFunction" }));
  private       DatabaseFactory            factory;
  private       Database                   database;
  private       Set<String>                edgeClasses                     = new HashSet<>();
  private       List<Map<String, Object>>  parsedUsers                     = new ArrayList<>();
  private       Map<RID, RID>              vertexRidMap                    = new HashMap<>();
  private       int                        batchSize                       = 10_000;
  private       PHASE                      phase                           = PHASE.OFF; // phase1 = create DB and cache edges in RAM, phase2 = create vertices and edges
  private       long                       processedItems                  = 0L;
  private       long                       skippedRecordBecauseNullKey     = 0L;
  private       long                       skippedEdgeBecauseMissingVertex = 0l;
  private       Map<String, Long>          totalEdgesByVertexType          = new HashMap<>();
  private       long                       beginTime;
  private       long                       beginTimeRecordsCreation;
  private       long                       beginTimeEdgeCreation;
  private       GZIPInputStream            inputStream;
  private       JsonReader                 reader;
  private       boolean                    error                           = false;
  private       ImporterContext            context                         = new ImporterContext();
  private       ImporterSettings           settings                        = new ImporterSettings();
  private       ImporterLogger             logger                          = new ImporterLogger(settings);

  private enum PHASE {OFF, CREATE_SCHEMA, CREATE_RECORDS, CREATE_EDGES}

  private class OrientDBClass {
    List<String>        superClasses = new ArrayList<>();
    List<Integer>       clusterIds   = new ArrayList<>();
    Map<String, String> properties   = new LinkedHashMap<>();
  }

  public OrientDBImporter(final String[] args) {
    printHeader();

    String state = null;
    for (String arg : args) {
      if (arg.equals("-?"))
        printHelp();
      else if (arg.equals("-d"))
        state = "databasePath";
      else if (arg.equals("-i"))
        state = "inputFile";
      else if (arg.equals("-s"))
        state = "securityFile";
      else if (arg.equals("-o"))
        overwriteDatabase = true;
      else if (arg.equals("-b"))
        state = "batchSize";
      else if (arg.equals("-v"))
        state = "verboseLevel";
      else {
        if (state.equals("databasePath"))
          databasePath = arg;
        else if (state.equals("inputFile"))
          inputFile = arg;
        else if (state.equals("securityFile"))
          securityFileName = arg;
        else if (state.equals("batchSize"))
          batchSize = Integer.parseInt(arg);
        else if (state.equals("verboseLevel"))
          settings.verboseLevel = Integer.parseInt(arg);
      }
    }

    if (inputFile == null)
      syntaxError("Missing input file. Use -f <file-path>");

    file = new File(inputFile);
  }

  public OrientDBImporter(final DatabaseInternal database) throws IOException {
    this.database = database;
    this.databasePath = database.getDatabasePath();
    this.file = null;
  }

  public static void main(final String[] args) throws IOException {
    new OrientDBImporter(args).run();
  }

  public GZIPInputStream openInputStream() throws IOException {
    return new GZIPInputStream(new FileInputStream(file));
  }

  public void run() throws IOException {
    if (file != null && !file.exists()) {
      error = true;
      throw new IllegalArgumentException("File '" + inputFile + "' not found");
    }

    final String from = inputFile != null ? "'" + inputFile + "'" : "stream";

    if (databasePath == null)
      logger.log(1, "Checking OrientDB database from %s...", from);
    else {
      logger.log(1, "Importing OrientDB database from %s to '%s'", from, databasePath);

      if (database == null) {
        factory = new DatabaseFactory(databasePath);
        if (factory.exists()) {
          if (!overwriteDatabase) {
            logger.error("Database already exists on path '%s'", databasePath);
            return;
          } else {
            database = factory.open();
            logger.error("Found existent database at '%s', dropping it and recreate a new one", databasePath);
            database.drop();
          }
        }

        // CREATE THE DATABASE
        database = factory.create();
      }
    }

    beginTime = System.currentTimeMillis();

    phase = PHASE.CREATE_SCHEMA;

    // PARSE THE FILE THE 1ST TIME TO CREATE THE SCHEMA AND CACHE EDGES IN RAM
    logger.log(1, "Creation of the schema: types, properties and indexes");
    parseInputFile();

    phase = PHASE.CREATE_EDGES;

    // PARSE THE FILE AGAIN TO CREATE RECORDS AND EDGES
    logger.log(1, "Creation of edges started: creating edges between vertices");
    beginTimeEdgeCreation = System.currentTimeMillis();
    parseInputFile();

    if (database.getSchema().existsType("V")) {
      if (database.countType("V", false) == 0) {
        logger.log(2, "Dropping empty 'V' base vertex type (in OrientDB all the vertices have their own class");
        database.getSchema().dropType("V");
      }
    }

    if (database.getSchema().existsType("E")) {
      if (database.countType("E", false) == 0) {
        logger.log(2, "Dropping empty 'E' base edge type (in OrientDB all the edges have their own class");
        database.getSchema().dropType("E");
      }
    }

    final long elapsed = (System.currentTimeMillis() - beginTime) / 1000;

    logger.log(1, "***************************************************************************************************");
    logger.log(1, "Import of OrientDB database completed in %,d secs with %,d errors and %,d warnings.", elapsed, errors, warnings);
    logger.log(1, "\nSUMMARY\n");
    logger.log(1, "- Records..................................: %,d", totalRecordParsed);
    for (Map.Entry<String, OrientDBClass> entry : classes.entrySet()) {
      final String className = entry.getKey();
      final Long recordsByClass = totalRecordByType.get(className);
      final long entries = recordsByClass != null ? recordsByClass : 0L;
      String additional = "";

      if (excludeClasses.contains(className))
        additional = " (excluded)";

      logger.log(1, "-- %-40s: %,d %s", className, entries, additional);
    }
    logger.log(1, "- Total attributes.........................: %,d", totalAttributesParsed);
    logger.log(1, "***************************************************************************************************");
    logger.log(1, "");
    logger.log(1, "NOTES:");

    if (securityFileName == null)
      logger.log(1,
          "- users stored in OUser class will not be imported because ArcadeDB has users only at server level. If you want to import such users into ArcadeDB server configuration, please run the importer with the option -s <securityFile>");
    else {
      writeSecurityFile();
      logger.log(1, "- you can find your security.json file to install in ArcadeDB server in '" + securityFileName + "'");
    }

    if (database != null)
      logger.log(1, "- you can find your new ArcadeDB database in '" + database.getDatabasePath() + "'");
  }

  public boolean isError() {
    return error;
  }

  public ImporterContext getContext() {
    return context;
  }

  public OrientDBImporter setContext(final ImporterContext context) {
    this.context = context;
    return this;
  }

  private void parseInputFile() throws IOException {
    inputStream = openInputStream();

    reader = new JsonReader(new InputStreamReader(inputStream));
    reader.setLenient(true);
    reader.beginObject();

    while (reader.hasNext()) {
      switch (reader.nextName()) {
      case "info":
        parseDatabaseInfo();
        break;

      case "clusters":
        parseClusters();
        break;

      case "schema":
        parseSchema();
        break;

      case "records":
        parseRecords();
        break;

      default:
        try {
          reader.skipValue();
        } catch (EOFException e) {
          return;
        }
      }
    }
    reader.endObject();
  }

  private void writeSecurityFile() throws IOException {
    final File securityFile = new File(securityFileName);

    final JSONObject securityFileContent = new JSONObject();

    final JSONObject users = new JSONObject();
    securityFileContent.put("users", users);

    for (Map<String, Object> u : parsedUsers) {
      String userName = (String) u.get("name");
      String password = (String) u.get("password");

      final JSONObject user = new JSONObject();

      final JSONArray databases = new JSONArray();
      databases.put(databaseName);
      user.put("name", userName);
      user.put("password", password);
      user.put("databases", databases);
      user.put("databaseBlackList", true);

      users.put(userName, user);
    }

    FileUtils.writeFile(securityFile, securityFileContent.toString());
  }

  private void parseRecords() throws IOException {
    reader.beginArray();

    processedItems = 0L;
    context.skippedEdges.set(0);
    context.parsed.set(0);

    database.begin();

    while (reader.peek() == BEGIN_OBJECT) {
      final Map<String, Object> attributes = parseRecord(reader, false);

      final String className = (String) attributes.get("@class");

      if (phase == PHASE.CREATE_SCHEMA || phase == PHASE.CREATE_RECORDS) {
        if (!edgeClasses.contains(className)) {
          createRecord(attributes);
        } else
          context.skippedEdges.incrementAndGet();

        if (processedItems > 0 && processedItems % 1_000_000 == 0) {
          final long elapsed = System.currentTimeMillis() - beginTimeRecordsCreation;
          logger.log(2, "- Status update: created %,d vertices and %,d documents, skipped %,d edges (%,d records/sec)", context.createdVertices.get(),
              context.createdDocuments.get(), context.skippedEdges.get(), ((context.createdDocuments.get() + context.createdVertices.get()) / elapsed * 1000));
        }

      } else {
        if (edgeClasses.contains(className)) {
          createEdges(attributes);
          if (processedItems > 0 && processedItems % 1_000_000 == 0) {
            final long elapsed = System.currentTimeMillis() - beginTimeEdgeCreation;
            logger.log(2, "- Status update: created %,d edges %s (%,d edges/sec)", context.createdEdges.get(), totalEdgesByVertexType,
                (context.createdEdges.get() / elapsed * 1000));
          }
        }
      }

      ++processedItems;

      if (processedItems > 0 && processedItems % batchSize == 0) {
        database.commit();
        database.begin();
      }

      if (reader.peek() == NULL)
        // FIX A BUG ON ORIENTDB EXPORTER WHEN GENERATE AN EMPTY RECORD
        reader.skipValue();
    }

    database.commit();

    final long elapsedInSecs = (System.currentTimeMillis() - beginTime) / 1000;

    switch (phase) {
    case CREATE_RECORDS:
      logger.log(1, "- Creation of records completed: created %,d vertices and %,d documents, skipped %,d edges (%,d records/sec elapsed=%,d secs)",
          context.createdVertices.get(), context.createdDocuments.get(), context.skippedEdges.get(),
          elapsedInSecs > 0 ? ((context.createdDocuments.get() + context.createdVertices.get()) / elapsedInSecs) : 0, elapsedInSecs);
      break;
    case CREATE_EDGES:
      logger.log(1, "- Creation of edges completed: created %,d edges %s (%,d edges/sec elapsed=%,d secs)", context.createdEdges.get(), totalEdgesByVertexType,
          elapsedInSecs > 0 ? (context.createdEdges.get() / elapsedInSecs) : 0, elapsedInSecs);
      break;
    default:
      error = true;
      throw new IllegalArgumentException("Invalid phase " + phase);
    }
    reader.endArray();
  }

  private MutableDocument createRecord(final Map<String, Object> attributes) throws IOException {
    MutableDocument record = null;

    final String rid = (String) attributes.get("@rid");
    if (rid == null)
      // EMBEDDED RECORD?
      return null;

    final RID recordRid = new RID(null, rid);
    final String recordType = (String) attributes.get("@type");
    if (recordType != null) {
      switch (recordType) {
      case "b":
        // BINARY
        if (recordRid.getBucketId() == 0 && recordRid.getPosition() == 0) {
          // INTERNAL ORIENTDB RECORD, IGNORE THEM
        } else {
          logger.error("- Unsupported binary record. Ignoring record: %s", attributes);
          ++warnings;
        }
        break;
      case "d":
        // DOCUMENT, VERTEX, EDGE
        final String className = (String) attributes.get("@class");
        if (className == null) {
          if (recordRid.getBucketId() == 0 && recordRid.getPosition() == 1) {
            // INTERNAL ORIENTDB RECORD SCHEMA, IGNORE THEM
          } else if (recordRid.getBucketId() == 0 && recordRid.getPosition() == 2)
            // INTERNAL ORIENTDB RECORD INDEX MGR, EXTRACT INDEXES
            parseIndexes(attributes);
          else {
            logger.error("- Unsupported record without class. Ignoring record: %s", attributes);
            ++warnings;
          }
        } else {
          if (className.equals("OUser"))
            parsedUsers.add(attributes);

          incrementRecordByClass(className);
          ++totalRecordParsed;
          context.parsed.incrementAndGet();

          if (!excludeClasses.contains(className)) {
            final DocumentType type = database.getSchema().getType(className);

            if (!checkForNullIndexes(attributes, type))
              return null;

            if (type instanceof VertexType)
              record = database.newVertex(className);
            else if (type instanceof EdgeType)
              // SKIP IT. EDGES ARE CREATED FROM VERTICES
              return null;
            else
              record = database.newDocument(className);

            for (Map.Entry<String, Object> entry : attributes.entrySet()) {
              final String attrName = entry.getKey();
              if (attrName.startsWith("@"))
                continue;

              Object attrValue = entry.getValue();

              if (attrValue instanceof Map) {
                //EMBEDDED
                attrValue = createRecord((Map<String, Object>) attrValue);
              } else if (attrValue instanceof List) {
                if (record instanceof Vertex && (attrName.startsWith("out_") || attrName.startsWith("in_")))
                  // EDGES WILL BE CREATE BELOW IN THE 2ND PHASE
                  attrValue = null;
              }

              if (attrValue != null)
                record.set(attrName, attrValue);
            }

            record.save();

            if (type instanceof VertexType)
              context.createdVertices.incrementAndGet();
            else
              context.createdDocuments.incrementAndGet();

            if (type instanceof VertexType) {
              // REMEMBER THE VERTEX TO ATTACH EDGES ON 2ND PHASE
              vertexRidMap.put(recordRid, record.getIdentity());
            }
          }
        }
        break;
      default:
        logger.error("- Unsupported record type '%s'", recordType);
      }
    }

    return record;
  }

  private void createEdges(final Map<String, Object> attributes) {
    final String recordType = (String) attributes.get("@type");
    if (recordType == null || !recordType.equals("d"))
      return;

    // DOCUMENT, VERTEX, EDGE
    final String className = (String) attributes.get("@class");
    if (className == null)
      return;

    if (excludeClasses.contains(className))
      return;

    final DocumentType type = database.getSchema().getType(className);
    if (!(type instanceof EdgeType))
      return;

    if (!checkForNullIndexes(attributes, type))
      return;

    Map<String, Object> properties = Collections.EMPTY_MAP;
    for (Map.Entry<String, Object> attr : attributes.entrySet())
      if (!attr.getKey().startsWith("@") && !attr.getKey().equals("out") && !attr.getKey().equals("in")) {
        if (properties == Collections.EMPTY_MAP)
          properties = new HashMap<>();
        properties.put(attr.getKey(), attr.getValue());
      }

    final RID out = new RID(database, (String) attributes.get("out"));
    final RID newOut = vertexRidMap.get(out);
    if (newOut == null) {
      ++skippedEdgeBecauseMissingVertex;

      if (settings.verboseLevel < 3 && skippedEdgeBecauseMissingVertex == 100)
        logger.log(2, "- Skipped 100 edges because one vertex is not in the database. Not reporting further case to reduce the output");
      else if (settings.verboseLevel > 2 || skippedEdgeBecauseMissingVertex < 100)
        logger.log(2, "- Skip edge %s because source vertex (out) was not imported", attributes);

      return;
    }

    final RID in = new RID(database, (String) attributes.get("in"));
    final RID newIn = vertexRidMap.get(in);
    if (newIn == null) {
      ++skippedEdgeBecauseMissingVertex;

      if (settings.verboseLevel < 3 && skippedEdgeBecauseMissingVertex == 100)
        logger.log(2, "- Skipped 100 edges because one vertex is not in the database. Not reporting further case to reduce the output");
      else if (settings.verboseLevel > 2 || skippedEdgeBecauseMissingVertex < 100)
        logger.log(2, "- Skip edge %s because destination vertex (in) was not imported", attributes);

      return;
    }

    final Vertex sourceVertex = (Vertex) database.lookupByRID(newOut, false);

    MutableEdge edge = sourceVertex.newEdge(className, newIn, true, properties);

    context.createdEdges.incrementAndGet();

    Long edgesByVertexType = totalEdgesByVertexType.get(className);
    if (edgesByVertexType == null) {
      edgesByVertexType = 0L;
      totalEdgesByVertexType.put(className, edgesByVertexType);
    }
    totalEdgesByVertexType.put(className, edgesByVertexType + 1);

    incrementRecordByClass(className);
  }

  private Map<String, Object> parseRecord(JsonReader reader, final boolean ignore) throws IOException {
    final Map<String, Object> attributes = ignore ? null : new LinkedHashMap<>();

    reader.beginObject();
    while (reader.peek() != END_OBJECT) {
      final String attributeName = reader.nextName();
      final Object attributeValue;

      final JsonToken propertyType = reader.peek();
      switch (propertyType) {
      case STRING:
        attributeValue = reader.nextString();
        break;
      case NUMBER:
        attributeValue = reader.nextDouble();
        break;
      case BOOLEAN:
        attributeValue = reader.nextBoolean();
        break;
      case NULL:
        reader.nextNull();
        attributeValue = null;
        break;
      case BEGIN_OBJECT:
        attributeValue = parseRecord(reader, ignore);
        break;
      case BEGIN_ARRAY:
        attributeValue = parseArray(reader, ignore);
        break;
      default:
        logger.log(2, "Skipping property '%s' of type '%s'", attributeName, propertyType);
        continue;
      }

      if (!ignore) {
        attributes.put(attributeName, attributeValue);
        ++totalAttributesParsed;
      }
    }

    reader.endObject();
    return attributes;
  }

  private List<Object> parseArray(JsonReader reader, final boolean ignore) throws IOException {
    final List<Object> list = ignore ? null : new ArrayList<>();
    reader.beginArray();
    while (reader.peek() != END_ARRAY) {
      final Object entryValue;

      final JsonToken entryType = reader.peek();
      switch (entryType) {
      case STRING:
        entryValue = reader.nextString();
        break;
      case NUMBER:
        entryValue = reader.nextDouble();
        break;
      case BOOLEAN:
        entryValue = reader.nextBoolean();
        break;
      case NULL:
        reader.nextNull();
        entryValue = null;
        break;
      case BEGIN_OBJECT:
        entryValue = parseRecord(reader, ignore);
        break;
      case BEGIN_ARRAY:
        entryValue = parseArray(reader, ignore);
        break;
      default:
        logger.log(2, "Skipping entry of type '%s'", entryType);
        continue;
      }

      if (!ignore)
        list.add(entryValue);
    }
    reader.endArray();
    return list;
  }

  private void parseSchema() throws IOException {
    reader.beginObject();

    while (reader.peek() != END_OBJECT) {
      final String name = reader.nextName();

      switch (name) {
      case "classes":
        reader.beginArray();

        while (reader.peek() != END_ARRAY) {
          reader.beginObject();

          String className = null;
          OrientDBClass cls = new OrientDBClass();

          while (reader.peek() != END_OBJECT) {
            switch (reader.nextName()) {
            case "name":
              className = reader.nextString();
              break;

            case "cluster-ids":
              reader.beginArray();
              while (reader.peek() != END_ARRAY)
                cls.clusterIds.add(reader.nextInt());
              reader.endArray();
              break;

            case "super-classes":
              reader.beginArray();
              while (reader.peek() != END_ARRAY)
                cls.superClasses.add(reader.nextString());
              reader.endArray();
              break;

            case "properties":
              String propertyName = null;
              String propertyType = null;

              reader.beginArray();
              while (reader.peek() != END_ARRAY) {
                reader.beginObject();

                while (reader.peek() != END_OBJECT) {
                  switch (reader.nextName()) {
                  case "name":
                    propertyName = reader.nextString();
                    break;
                  case "type":
                    propertyType = reader.nextString();
                    break;
                  default:
                    reader.skipValue();
                  }
                }

                cls.properties.put(propertyName, propertyType);
                reader.endObject();
              }
              reader.endArray();
              break;

            default:
              reader.skipValue();
            }
          }
          reader.endObject();

          classes.put(className, cls);
        }
        reader.endArray();
        break;

      default:
        reader.skipValue();
      }
    }

    reader.endObject();

    for (String className : classes.keySet())
      createType(className);
  }

  private void parseIndexes(final Map<String, Object> attributes) throws IOException {
    final List<Map<String, Object>> parsedIndexes = (List<Map<String, Object>>) attributes.get("indexes");
    for (Map<String, Object> parsedIndex : parsedIndexes) {
      final String indexName = (String) parsedIndex.get("name");
      final boolean unique = parsedIndex.get("type").toString().startsWith("UNIQUE");

      final Map<String, Object> indexDefinition = (Map<String, Object>) parsedIndex.get("indexDefinition");
      final String className = (String) indexDefinition.get("className");
      final String keyType = (String) indexDefinition.get("keyType");

      if (!classes.containsKey(className))
        continue;

      if (excludeClasses.contains(className))
        continue;

      final String fieldName = (String) indexDefinition.get("field");
      final String[] properties = new String[] { fieldName };
      final boolean nullValuesIgnored = (boolean) indexDefinition.get("nullValuesIgnored");

      final DocumentType type = database.getSchema().getType(className);
      if (!type.existsProperty(fieldName)) {
        if (keyType == null) {
          logger.log(2, "- Skipped %s index creation on %s%s because the property is not defined and the key type is unknown", unique ? "UNIQUE" : "NOT UNIQUE",
              className, Arrays.toString(properties));
          continue;
        }

        type.createProperty(fieldName, Type.valueOf(keyType));
      }

      database.getSchema().createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, unique, className, properties, LSMTreeIndexAbstract.DEF_PAGE_SIZE,
          nullValuesIgnored ? LSMTreeIndexAbstract.NULL_STRATEGY.SKIP : LSMTreeIndexAbstract.NULL_STRATEGY.ERROR, null);

      logger.log(2, "- Created index %s on %s%s", unique ? "UNIQUE" : "NOT UNIQUE", className, Arrays.toString(properties));
    }

    if (phase == PHASE.CREATE_SCHEMA) {
      logger.log(1, "Creation of records started: creating vertices and documents records (edges on the next phase)");
      phase = PHASE.CREATE_RECORDS;
      beginTimeRecordsCreation = System.currentTimeMillis();
    }
  }

  private void createType(final String className) {
    final OrientDBClass classInfo = classes.get(className);

    int type;
    if (className.equals("V")) {
      type = 1;
    } else if (className.equals("E")) {
      type = 2;
    } else
      type = 0;

    if (!classInfo.superClasses.isEmpty()) {
      for (String c : classInfo.superClasses)
        createType(c);

      type = getClassType(classInfo.superClasses);
    }

    if (database.getSchema().existsType(className))
      // ALREADY CREATED
      return;

    if (excludeClasses.contains(className))
      // SPECIAL ORIENTDB CLASSES, NO NEED IN ARCADEDB
      return;

    final DocumentType t;

    switch (type) {
    case 1:
      t = database.getSchema().createVertexType(className);
      break;

    case 2:
      t = database.getSchema().createEdgeType(className);
      edgeClasses.add(className);
      break;

    default:
      t = database.getSchema().createDocumentType(className);
    }

    // CREATE PROPERTIES
    for (Map.Entry<String, String> entry : classInfo.properties.entrySet()) {
      String orientdbType = entry.getValue();

      switch (orientdbType) {
      case "EMBEDDEDLIST":
      case "EMBEDDEDSET":
      case "LINKLIST":
      case "LINKSET":
        orientdbType = "LIST";
        break;
      case "EMBEDDEDMAP":
      case "LINKMAP":
        orientdbType = "MAP";
        break;
      }

      t.createProperty(entry.getKey(), Type.valueOf(orientdbType));
    }

    logger.log(2, "- Created type '%s' with the following properties %s", className, classInfo.properties);
  }

  private int getClassType(List<String> list) {
    int type = 0;
    for (int i = 0; type == 0 && i < list.size(); ++i) {
      final String su = list.get(i);
      if (su.equals("V")) {
        type = 1;
        break;
      } else if (su.equals("E")) {
        type = 2;
        break;
      }

      final OrientDBClass c = classes.get(su);
      if (c != null) {
        if (!c.superClasses.isEmpty())
          type = getClassType(c.superClasses);
      }
    }
    return type;
  }

  private void parseClusters() throws IOException {
    reader.beginArray();
    while (reader.hasNext()) {
      reader.beginObject();

      String clusterName = null;
      int clusterId = -1;

      while (reader.hasNext()) {
        switch (reader.nextName()) {
        case "name":
          clusterName = reader.nextString();
          break;

        case "id":
          clusterId = reader.nextInt();
          break;

        default:
          reader.skipValue();
        }

        if (clusterName != null && clusterId > -1) {
          clustersNameToId.put(clusterName, clusterId);
          clustersIdToName.put(clusterId, clusterName);

          clusterName = null;
          clusterId = -1;
          skipUntilEndOfObject(reader, END_OBJECT);

          if (reader.peek() != BEGIN_OBJECT)
            break;
          reader.beginObject();
        }
      }
      reader.endObject();
    }
    reader.endArray();
  }

  private void parseDatabaseInfo() throws IOException {
    reader.beginObject();

    switch (reader.nextName()) {
    case "name":
      databaseName = reader.nextString();
      break;

    default:
      reader.skipValue();
    }

    skipUntilEndOfObject(reader, END_OBJECT);
    reader.endObject();
  }

  private void skipUntilEndOfObject(final JsonReader reader, final JsonToken token) throws IOException {
    while (reader.hasNext()) {
      final JsonToken t = reader.peek();
      reader.skipValue();

      if (t == token)
        return;
    }
  }

  private void skipUntilPropertyName(final JsonReader reader, final String propertyName) throws IOException {
    while (reader.hasNext()) {
      final JsonToken t = reader.peek();

      if (t == NAME) {
        if (reader.nextName().equals(propertyName))
          return;
      } else
        reader.skipValue();
    }
  }

  private void syntaxError(final String s) {
    logger.error("Syntax error: " + s);
    error = true;
    printHelp();
  }

  private void printHeader() {
    logger.log(1, Constants.PRODUCT + " " + Constants.getVersion() + " - OrientDB Importer");
  }

  private void printHelp() {
    logger.error("Use:");
    logger.error("-d <database-path>: create a database from the OrientDB export");
    logger.error("-i <input-file>: path to the OrientDB export file. The default name is export.json.gz");
    logger.error("-s <security-file>: path to the security file generated from OrientDB users to use in ArcadeDB server");
    logger.error("-v <verbose-level>: 0 = error only, 1 = main steps, 2 = detailed steps, 3 = everything");
  }

  private boolean checkForNullIndexes(final Map<String, Object> properties, final DocumentType type) {
    boolean valid = true;
    final List<Index> indexes = type.getAllIndexes(true);
    for (Index index : indexes) {
      if (index.getNullStrategy() == LSMTreeIndexAbstract.NULL_STRATEGY.ERROR) {
        final Object value = properties.get(index.getPropertyNames()[0]);
        if (value == null) {
          ++skippedRecordBecauseNullKey;

          if (settings.verboseLevel < 3 && skippedRecordBecauseNullKey == 100)
            logger.log(2,
                "- Skipped 100 records where indexed field is null and the index is not accepting NULLs. Not reporting further case to reduce the output");
          else if (settings.verboseLevel > 2 || skippedRecordBecauseNullKey < 100)
            logger.log(2, "- Skipped record %s because indexed field '%s' is null and the index is not accepting NULLs", properties,
                index.getPropertyNames()[0]);

          valid = false;
        }
      }
    }

    return valid;
  }

  private void incrementRecordByClass(final String className) {
    Long recordsByClass = totalRecordByType.get(className);
    if (recordsByClass == null) {
      recordsByClass = 0L;
      totalRecordByType.put(className, recordsByClass);
    }
    totalRecordByType.put(className, recordsByClass + 1);
  }
}
