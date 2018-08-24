/*
 * Copyright (c) 2018 - Arcade Analytics LTD (https://arcadeanalytics.com)
 */

package com.arcadedb.schema;

import com.arcadedb.Constants;
import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.engine.*;
import com.arcadedb.engine.Dictionary;
import com.arcadedb.exception.ConfigurationException;
import com.arcadedb.exception.DatabaseMetadataException;
import com.arcadedb.exception.SchemaException;
import com.arcadedb.index.Index;
import com.arcadedb.index.IndexFactory;
import com.arcadedb.index.lsm.LSMTreeIndex;
import com.arcadedb.index.lsm.LSMTreeIndexMutable;
import com.arcadedb.serializer.BinaryTypes;
import com.arcadedb.utility.FileUtils;
import com.arcadedb.utility.LogManager;
import org.json.JSONArray;
import org.json.JSONObject;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.Callable;

public class SchemaImpl implements Schema {
  public static final String DEFAULT_DATE_FORMAT     = "yyyy-MM-dd";
  public static final String DEFAULT_DATETIME_FORMAT = "yyyy-MM-dd HH:mm:ss";
  public static final String DEFAULT_ENCODING        = "UTF-8";

  public static final String                    SCHEMA_FILE_NAME = "schema.json";
  private final       DatabaseInternal          database;
  private final       List<PaginatedComponent>  files            = new ArrayList<PaginatedComponent>();
  private final       Map<String, DocumentType> types            = new HashMap<String, DocumentType>();
  private final       Map<String, Bucket>       bucketMap        = new HashMap<String, Bucket>();
  private final       Map<String, Index>        indexMap         = new HashMap<String, Index>();
  private final       String                    databasePath;
  private             Dictionary                dictionary;
  private             String                    dateFormat       = DEFAULT_DATE_FORMAT;
  private             String                    dateTimeFormat   = DEFAULT_DATETIME_FORMAT;
  private             String                    encoding         = DEFAULT_ENCODING;
  private             TimeZone                  timeZone         = TimeZone.getDefault();
  private final       PaginatedComponentFactory paginatedComponentFactory;
  private final       IndexFactory              indexFactory     = new IndexFactory();

  public enum INDEX_TYPE {
    LSM_TREE, LSM_HASH
  }

  public SchemaImpl(final DatabaseInternal database, final String databasePath, final PaginatedFile.MODE mode) {
    this.database = database;
    this.databasePath = databasePath;

    paginatedComponentFactory = new PaginatedComponentFactory(database);
    paginatedComponentFactory.registerComponent(Dictionary.DICT_EXT, new Dictionary.PaginatedComponentFactoryHandler());
    paginatedComponentFactory.registerComponent(Bucket.BUCKET_EXT, new Bucket.PaginatedComponentFactoryHandler());
    paginatedComponentFactory.registerComponent(LSMTreeIndexMutable.UNIQUE_INDEX_EXT, new LSMTreeIndexMutable.PaginatedComponentFactoryHandlerUnique());
    paginatedComponentFactory.registerComponent(LSMTreeIndexMutable.NOTUNIQUE_INDEX_EXT, new LSMTreeIndexMutable.PaginatedComponentFactoryHandlerNotUnique());

    indexFactory.register(INDEX_TYPE.LSM_TREE.name(), new LSMTreeIndexMutable.IndexFactoryHandler());
  }

  public void create(final PaginatedFile.MODE mode) {
    database.begin();
    try {
      dictionary = new Dictionary(database, "dictionary", databasePath + "/dictionary", mode, Dictionary.DEF_PAGE_SIZE);
      files.add(dictionary);

      database.commit();

    } catch (Exception e) {
      LogManager.instance().error(this, "Error on opening dictionary '%s' (error=%s)", e, databasePath, e.toString());
      database.rollback();
      throw new DatabaseMetadataException("Error on loading dictionary (error=" + e.toString() + ")", e);
    }
  }

  public void load(final PaginatedFile.MODE mode) throws IOException {
    final Collection<PaginatedFile> filesToOpen = database.getFileManager().getFiles();

    // REGISTER THE DICTIONARY FIRST
    for (PaginatedFile file : filesToOpen) {
      if (Dictionary.DICT_EXT.equals(file.getFileExtension())) {
        dictionary = (Dictionary) paginatedComponentFactory.createComponent(file, mode);
        registerFile(dictionary);
        break;
      }
    }

    if (dictionary == null)
      throw new ConfigurationException("Dictionary file not found in database directory");

    for (PaginatedFile file : filesToOpen) {
      if (!Dictionary.DICT_EXT.equals(file.getFileExtension())) {
        final PaginatedComponent pf = paginatedComponentFactory.createComponent(file, mode);

        if (pf instanceof Bucket)
          bucketMap.put(pf.getName(), (Bucket) pf);
        else if (pf instanceof Index)
          indexMap.put(pf.getName(), (Index) pf);

        if (pf != null)
          registerFile(pf);
      }
    }

    for (PaginatedComponent f : files)
      if (f != null)
        f.onAfterLoad();

    readConfiguration();
  }

  @Override
  public TimeZone getTimeZone() {
    return timeZone;
  }

  @Override
  public void setTimeZone(final TimeZone timeZone) {
    this.timeZone = timeZone;
  }

  @Override
  public String getDateFormat() {
    return dateFormat;
  }

  @Override
  public void setDateFormat(final String dateFormat) {
    this.dateFormat = dateFormat;
  }

  @Override
  public String getDateTimeFormat() {
    return dateTimeFormat;
  }

  @Override
  public void setDateTimeFormat(final String dateTimeFormat) {
    this.dateTimeFormat = dateTimeFormat;
  }

  @Override
  public PaginatedComponent getFileById(final int id) {
    if (id >= files.size())
      throw new SchemaException("File with id '" + id + "' was not found");

    final PaginatedComponent p = files.get(id);
    if (p == null)
      throw new SchemaException("File with id '" + id + "' was not found");
    return p;
  }

  public void removeFile(final int fileId) {
    if (fileId >= files.size())
      return;
    files.set(fileId, null);
  }

  @Override
  public Collection<Bucket> getBuckets() {
    return Collections.unmodifiableCollection(bucketMap.values());
  }

  public boolean existsBucket(final String bucketName) {
    return bucketMap.containsKey(bucketName);
  }

  @Override
  public Bucket getBucketByName(final String name) {
    final Bucket p = bucketMap.get(name);
    if (p == null)
      throw new SchemaException("Bucket with name '" + name + "' was not found");
    return p;
  }

  @Override
  public Bucket getBucketById(final int id) {
    if (id < 0 || id >= files.size())
      throw new SchemaException("Bucket with id '" + id + "' was not found");

    final PaginatedComponent p = files.get(id);
    if (p == null || !(p instanceof Bucket))
      throw new SchemaException("Bucket with id '" + id + "' was not found");
    return (Bucket) p;
  }

  @Override
  public Bucket createBucket(final String bucketName) {
    return createBucket(bucketName, Bucket.DEF_PAGE_SIZE);
  }

  public Bucket createBucket(final String bucketName, final int pageSize) {
    return (Bucket) database.executeInWriteLock(new Callable<Object>() {
      @Override
      public Object call() {
        if (bucketMap.containsKey(bucketName))
          throw new SchemaException("Cannot create bucket '" + bucketName + "' because already exists");

        try {
          final Bucket bucket = new Bucket(database, bucketName, databasePath + "/" + bucketName, PaginatedFile.MODE.READ_WRITE, pageSize);
          registerFile(bucket);
          bucketMap.put(bucketName, bucket);

          return bucket;

        } catch (IOException e) {
          throw new SchemaException("Cannot create bucket '" + bucketName + "' (error=" + e + ")", e);
        }
      }
    });
  }

  public String getEncoding() {
    return encoding;
  }

  @Override
  public boolean existsIndex(final String indexName) {
    return indexMap.containsKey(indexName);
  }

  @Override
  public Index[] getIndexes() {
    final Index[] indexes = new Index[indexMap.size()];
    int i = 0;
    for (Index index : indexMap.values())
      indexes[i++] = index;
    return indexes;
  }

  public void removeIndex(final String indexName) {
    indexMap.remove(indexName);
  }

  @Override
  public Index getIndexByName(final String indexName) {
    final Index p = indexMap.get(indexName);
    if (p == null)
      throw new SchemaException("Index with name '" + indexName + "' was not found");
    return p;
  }

  @Override
  public Index[] createClassIndexes(final INDEX_TYPE indexType, final boolean unique, final String typeName, final String[] propertyNames) {
    return createClassIndexes(indexType, unique, typeName, propertyNames, LSMTreeIndex.DEF_PAGE_SIZE);
  }

  @Override
  public Index[] createClassIndexes(final INDEX_TYPE indexType, final boolean unique, final String typeName, final String[] propertyNames, final int pageSize) {
    return (Index[]) database.executeInWriteLock(new Callable<Object>() {
      @Override
      public Object call() {
        try {
          if (propertyNames.length == 0)
            throw new DatabaseMetadataException("Cannot create index on type '" + typeName + "' because there are no property defined");

          final DocumentType type = getType(typeName);

          // CHECK ALL THE PROPERTIES EXIST
          final byte[] keyTypes = new byte[propertyNames.length];
          int i = 0;

          for (String propertyName : propertyNames) {
            final Property property = type.getPolymorphicProperty(propertyName);
            if (property == null)
              throw new SchemaException("Cannot create the index on type '" + typeName + "." + propertyName + "' because the property does not exist");

            keyTypes[i++] = BinaryTypes.getTypeFromClass(property.getType());
          }

          final List<Bucket> buckets = type.getBuckets(false);

          final Index[] indexes = new Index[buckets.size()];
          for (int idx = 0; idx < buckets.size(); ++idx) {
            final Bucket b = buckets.get(idx);
            final String indexName = b.getName() + "_" + System.nanoTime();

            if (indexMap.containsKey(indexName))
              throw new DatabaseMetadataException("Cannot create index '" + indexName + "' on type '" + typeName + "' because it already exists");

            indexes[idx] = indexFactory
                .createIndex(indexType.name(), database, indexName, unique, databasePath + "/" + indexName, PaginatedFile.MODE.READ_WRITE, keyTypes, pageSize);

            if (indexes[idx] instanceof PaginatedComponent)
              registerFile((PaginatedComponent) indexes[idx]);

            indexMap.put(indexName, indexes[idx]);

            type.addIndexInternal(indexes[idx], b, propertyNames);
          }

          saveConfiguration();

          return indexes;

        } catch (IOException e) {
          throw new SchemaException("Cannot create index on type '" + typeName + "' (error=" + e + ")", e);
        }
      }
    });
  }

  public Index createManualIndex(final INDEX_TYPE indexType, final boolean unique, final String indexName, final byte[] keyTypes, final int pageSize) {
    return (LSMTreeIndexMutable) database.executeInWriteLock(new Callable<Object>() {
      @Override
      public Object call() {
        if (indexMap.containsKey(indexName))
          throw new SchemaException("Cannot create index '" + indexName + "' because already exists");

        try {
          Index index = indexFactory
              .createIndex(indexType.name(), database, indexName, unique, databasePath + "/" + indexName, PaginatedFile.MODE.READ_WRITE, keyTypes, pageSize);

          if (index instanceof PaginatedComponent)
            registerFile((PaginatedComponent) index);

          indexMap.put(indexName, index);

          return index;

        } catch (IOException e) {
          throw new SchemaException("Cannot create index '" + indexName + "' (error=" + e + ")", e);
        }
      }
    });
  }

  public void close() {
    files.clear();
    types.clear();
    bucketMap.clear();
    for (Index i : indexMap.values())
      i.close();
    indexMap.clear();
    dictionary = null;
  }

  public Dictionary getDictionary() {
    return dictionary;
  }

  public Database getDatabase() {
    return database;
  }

  public Collection<DocumentType> getTypes() {
    return Collections.unmodifiableCollection(types.values());
  }

  public DocumentType getType(final String typeName) {
    final DocumentType t = types.get(typeName);
    if (t == null)
      throw new SchemaException("Type with name '" + typeName + "' was not found");
    return t;
  }

  @Override
  public String getTypeNameByBucketId(final int bucketId) {
    for (DocumentType t : types.values()) {
      for (Bucket b : t.getBuckets(false)) {
        if (b.getId() == bucketId)
          return t.getName();
      }
    }

    // NOT FOUND
    return null;
  }

  @Override
  public DocumentType getTypeByBucketId(final int bucketId) {
    for (DocumentType t : types.values()) {
      for (Bucket b : t.getBuckets(false)) {
        if (b.getId() == bucketId)
          return t;
      }
    }

    // NOT FOUND
    return null;
  }

  public boolean existsType(final String typeName) {
    return types.containsKey(typeName);
  }

  public void dropType(final String typeName) {
    database.executeInWriteLock(new Callable<Object>() {
      @Override
      public Object call() {
        if (types.remove(typeName) == null)
          throw new SchemaException("Type '" + typeName + "' not found");

        saveConfiguration();
        return null;
      }
    });
  }

  public DocumentType createDocumentType(final String typeName) {
    return createDocumentType(typeName, Runtime.getRuntime().availableProcessors());
  }

  public DocumentType createDocumentType(final String typeName, final int buckets) {
    return createDocumentType(typeName, buckets, Bucket.DEF_PAGE_SIZE);
  }

  public DocumentType createDocumentType(final String typeName, final int buckets, final int pageSize) {
    return (DocumentType) database.executeInWriteLock(new Callable<Object>() {
      @Override
      public Object call() {
        if (typeName.indexOf(",") > -1)
          throw new IllegalArgumentException("Type name '" + typeName + "' contains non valid characters");

        if (types.containsKey(typeName))
          throw new SchemaException("Type '" + typeName + "' already exists");
        final DocumentType c = new DocumentType(SchemaImpl.this, typeName);
        types.put(typeName, c);

        for (int i = 0; i < buckets; ++i)
          c.addBucket(createBucket(typeName + "_" + i, pageSize));

        saveConfiguration();

        return c;
      }
    });
  }

  @Override
  public VertexType createVertexType(final String typeName) {
    return createVertexType(typeName, Runtime.getRuntime().availableProcessors());
  }

  @Override
  public VertexType createVertexType(final String typeName, final int buckets) {
    return createVertexType(typeName, buckets, Bucket.DEF_PAGE_SIZE);
  }

  @Override
  public VertexType createVertexType(String typeName, final int buckets, final int pageSize) {
    return (VertexType) database.executeInWriteLock(new Callable<Object>() {
      @Override
      public Object call() {
        if (typeName.indexOf(",") > -1)
          throw new IllegalArgumentException("Vertex type name '" + typeName + "' contains non valid characters");

        if (types.containsKey(typeName))
          throw new SchemaException("Vertex type '" + typeName + "' already exists");
        final DocumentType c = new VertexType(SchemaImpl.this, typeName);
        types.put(typeName, c);

        for (int i = 0; i < buckets; ++i)
          c.addBucket(createBucket(typeName + "_" + i, pageSize));

        database.getGraphEngine().createVertexType(database, c);

        saveConfiguration();

        return c;
      }
    });
  }

  @Override
  public EdgeType createEdgeType(final String typeName) {
    return createEdgeType(typeName, Runtime.getRuntime().availableProcessors());
  }

  @Override
  public EdgeType createEdgeType(final String typeName, final int buckets) {
    return createEdgeType(typeName, buckets, Bucket.DEF_PAGE_SIZE);
  }

  @Override
  public EdgeType createEdgeType(final String typeName, final int buckets, final int pageSize) {
    return (EdgeType) database.executeInWriteLock(new Callable<Object>() {
      @Override
      public Object call() {
        if (typeName.indexOf(",") > -1)
          throw new IllegalArgumentException("Edge type name '" + typeName + "' contains non valid characters");

        if (types.containsKey(typeName))
          throw new SchemaException("Edge type '" + typeName + "' already exists");
        final DocumentType c = new EdgeType(SchemaImpl.this, typeName);
        types.put(typeName, c);

        for (int i = 0; i < buckets; ++i)
          c.addBucket(createBucket(typeName + "_" + i, pageSize));

        saveConfiguration();

        return c;
      }
    });
  }

  public void swapIndexes(final LSMTreeIndexMutable oldIndex, final LSMTreeIndexMutable newIndex) {
    oldIndex.lazyDrop();

    indexMap.put(newIndex.getName(), newIndex);

    LogManager.instance().info(this, "Replacing index '%s' -> '%s' in schema...", oldIndex.getName(), newIndex.getName());

    // SCAN ALL THE TYPES TO FIND WHERE THE INDEX WAS DEFINED TO REPLACE IT
    for (DocumentType t : getTypes())
      t.replaceIndex(oldIndex, newIndex);

    newIndex.removeTempSuffix();

    saveConfiguration();
  }

  protected void readConfiguration() throws IOException {
    types.clear();

    final File file = new File(databasePath + "/" + SCHEMA_FILE_NAME);
    if (!file.exists())
      return;

    final String fileContent = FileUtils.readStreamAsString(new FileInputStream(file), encoding);

    final JSONObject root = new JSONObject(fileContent);

    //root.get("version", PConstants.VERSION);

    final JSONObject settings = root.getJSONObject("settings");

    timeZone = TimeZone.getTimeZone(settings.getString("timeZone"));
    dateFormat = settings.getString("dateFormat");
    dateTimeFormat = settings.getString("dateTimeFormat");

    final JSONObject types = root.getJSONObject("types");

    final Map<String, String[]> parentTypes = new HashMap<>();

    for (String typeName : types.keySet()) {
      final JSONObject schemaType = types.getJSONObject(typeName);

      final DocumentType type;

      final String kind = (String) schemaType.get("type");
      if ("v".equals(kind)) {
        type = new VertexType(this, typeName);
      } else if ("e".equals(kind)) {
        type = new EdgeType(this, typeName);
      } else if ("d".equals(kind)) {
        type = new DocumentType(this, typeName);
      } else
        throw new ConfigurationException("Type '" + kind + "' is not supported");

      this.types.put(typeName, type);

      final JSONArray schemaParent = schemaType.getJSONArray("parents");
      if (schemaParent != null) {
        // SAVE THE PARENT HIERARCHY FOR LATER
        final String[] parents = new String[schemaParent.length()];
        parentTypes.put(typeName, parents);
        for (int i = 0; i < schemaParent.length(); ++i)
          parents[i] = schemaParent.getString(i);
      }

      final JSONArray schemaBucket = schemaType.getJSONArray("buckets");
      if (schemaBucket != null) {
        for (int i = 0; i < schemaBucket.length(); ++i) {
          final PaginatedComponent bucket = bucketMap.get(schemaBucket.getString(i));
          if (bucket == null || !(bucket instanceof Bucket))
            LogManager.instance().warn(this, "Cannot find bucket %s for type '%s'", schemaBucket.getInt(i), type);
          type.addBucketInternal((Bucket) bucket);
        }
      }

      final JSONObject schemaIndexes = schemaType.getJSONObject("indexes");
      if (schemaIndexes != null) {
        for (String indexName : schemaIndexes.keySet()) {
          final JSONObject index = schemaIndexes.getJSONObject(indexName);

          final JSONArray schemaProperties = index.getJSONArray("properties");

          final String[] properties = new String[schemaProperties.length()];
          for (int i = 0; i < properties.length; ++i)
            properties[i] = schemaProperties.getString(i);
          type.addIndexInternal(getIndexByName(indexName), bucketMap.get(index.getString("bucket")), properties);
        }
      }
    }

    // RESTORE THE INHERITANCE
    for (Map.Entry<String, String[]> entry : parentTypes.entrySet()) {
      final DocumentType type = getType(entry.getKey());
      for (String p : entry.getValue())
        type.addParent(getType(p));
    }
  }

  public void saveConfiguration() {
    try {
      final FileWriter file = new FileWriter(databasePath + "/" + SCHEMA_FILE_NAME);

      final JSONObject root = new JSONObject();
      root.put("version", Constants.VERSION);

      final JSONObject settings = new JSONObject();
      root.put("settings", settings);

      settings.put("timeZone", timeZone.getDisplayName());
      settings.put("dateFormat", dateFormat);
      settings.put("dateTimeFormat", dateTimeFormat);

      final JSONObject types = new JSONObject();
      root.put("types", types);

      for (DocumentType t : this.types.values()) {
        final JSONObject type = new JSONObject();
        types.put(t.getName(), type);

        final String kind;
        if (t instanceof VertexType)
          kind = "v";
        else if (t instanceof EdgeType)
          kind = "e";
        else
          kind = "d";
        type.put("type", kind);

        final String[] parents = new String[t.getParentTypes().size()];
        for (int i = 0; i < parents.length; ++i)
          parents[i] = t.getParentTypes().get(i).getName();
        type.put("parents", parents);

        final List<Bucket> originalBuckets = t.getBuckets(false);
        final String[] buckets = new String[originalBuckets.size()];
        for (int i = 0; i < buckets.length; ++i)
          buckets[i] = originalBuckets.get(i).getName();

        type.put("buckets", buckets);

        final JSONObject indexes = new JSONObject();
        type.put("indexes", indexes);

        for (List<DocumentType.IndexMetadata> list : t.getAllIndexesMetadata()) {
          for (DocumentType.IndexMetadata entry : list) {
            final JSONObject index = new JSONObject();
            indexes.put(entry.index.getName(), index);

            index.put("bucket", getBucketById(entry.bucketId).getName());
            index.put("properties", entry.propertyNames);
          }
        }
      }

      file.write(root.toString());
      file.close();

    } catch (IOException e) {
      LogManager.instance().error(this, "Error on saving schema configuration to file: %s", e, databasePath + "/" + SCHEMA_FILE_NAME);
    }
  }

  public void registerFile(final PaginatedComponent file) {
    while (files.size() < file.getId() + 1)
      files.add(null);

    if (files.get(file.getId()) != null)
      throw new SchemaException("File with id '" + file.getId() + "' already exists (" + files.get(file.getId()) + ")");

    files.set(file.getId(), file);
  }
}
