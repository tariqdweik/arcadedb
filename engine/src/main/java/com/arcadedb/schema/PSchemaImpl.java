package com.arcadedb.schema;

import com.arcadedb.PConstants;
import com.arcadedb.database.PDatabase;
import com.arcadedb.database.PDatabaseInternal;
import com.arcadedb.engine.PBucket;
import com.arcadedb.engine.PDictionary;
import com.arcadedb.engine.PPaginatedComponent;
import com.arcadedb.engine.PPaginatedFile;
import com.arcadedb.exception.PConfigurationException;
import com.arcadedb.exception.PDatabaseMetadataException;
import com.arcadedb.exception.PSchemaException;
import com.arcadedb.index.PIndex;
import com.arcadedb.index.PIndexLSM;
import com.arcadedb.serializer.PBinaryTypes;
import com.arcadedb.utility.PFileUtils;
import com.arcadedb.utility.PLogManager;
import org.json.JSONArray;
import org.json.JSONObject;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.Callable;

public class PSchemaImpl implements PSchema {
  public static final String DEFAULT_DATE_FORMAT     = "yyyy-MM-dd";
  public static final String DEFAULT_DATETIME_FORMAT = "yyyy-MM-dd HH:mm:ss";
  public static final String DEFAULT_ENCODING        = "UTF-8";

  private static final String                     SCHEMA_FILE_NAME = "/schema.json";
  private final        PDatabaseInternal          database;
  private final        List<PPaginatedComponent>  files            = new ArrayList<PPaginatedComponent>();
  private final        Map<String, PDocumentType> types            = new HashMap<String, PDocumentType>();
  private final        Map<String, PBucket>       bucketMap        = new HashMap<String, PBucket>();
  private final        Map<String, PIndex>        indexMap         = new HashMap<String, PIndex>();
  private final        String                     databasePath;
  private              PDictionary                dictionary;
  private              String                     dateFormat       = DEFAULT_DATE_FORMAT;
  private              String                     dateTimeFormat   = DEFAULT_DATETIME_FORMAT;
  private              String                     encoding         = DEFAULT_ENCODING;
  private              TimeZone                   timeZone         = TimeZone.getDefault();

  public PSchemaImpl(final PDatabaseInternal database, final String databasePath, final PPaginatedFile.MODE mode) {
    this.database = database;
    this.databasePath = databasePath;
  }

  public void create(final PPaginatedFile.MODE mode) {
    database.begin();
    try {
      dictionary = new PDictionary(database, "dictionary", databasePath + "/dictionary", mode, PDictionary.DEF_PAGE_SIZE);
      files.add(dictionary);

      database.commit();

    } catch (Exception e) {
      PLogManager.instance().error(this, "Error on opening dictionary '%s' (error=%s)", e, databasePath, e.toString());
      database.rollback();
      throw new PDatabaseMetadataException("Error on loading dictionary (error=" + e.toString() + ")", e);
    }
  }

  public void load(final PPaginatedFile.MODE mode) throws IOException {
    for (PPaginatedFile file : database.getFileManager().getFiles()) {
      final String fileName = file.getFileName();
      final int fileId = file.getFileId();
      final String fileExt = file.getFileExtension();
      final int pageSize = file.getPageSize();

      PPaginatedComponent pf = null;

      if (fileExt.equals(PDictionary.DICT_EXT)) {
        // DICTIONARY
        try {
          dictionary = new PDictionary(database, fileName, file.getFilePath(), fileId, mode, pageSize);
          pf = dictionary;

        } catch (IOException e) {
          PLogManager.instance().error(this, "Error on opening dictionary '%s' (error=%s)", e, file, e.toString());
        }

      } else if (fileExt.equals(PBucket.BUCKET_EXT)) {
        // BUCKET
        try {
          final PBucket newPart = new PBucket(database, fileName, file.getFilePath(), fileId, mode, pageSize);
          bucketMap.put(fileName, newPart);
          pf = newPart;
        } catch (IOException e) {
          PLogManager.instance().error(this, "Error on opening bucket '%s' (error=%s)", e, file, e.toString());
        }

      } else if (fileExt.equals(PIndexLSM.INDEX_EXT)) {
        // INDEX
        try {
          final PIndexLSM index = new PIndexLSM(database, fileName, file.getFilePath(), fileId, mode, pageSize);
          indexMap.put(fileName, index);
          pf = index;
        } catch (IOException e) {
          PLogManager.instance().error(this, "Error on opening index '%s' (error=%s)", e, file, e.toString());
        }
      }

      if (pf != null)
        registerFile(pf);
    }

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
  public PPaginatedComponent getFileById(final int id) {
    if (id >= files.size())
      throw new PSchemaException("File with id '" + id + "' was not found");

    final PPaginatedComponent p = files.get(id);
    if (p == null)
      throw new PSchemaException("File with id '" + id + "' was not found");
    return p;
  }

  @Override
  public void removeFile(final int fileId) {
    if (fileId >= files.size())
      return;
    files.set(fileId, null);
  }

  @Override
  public Collection<PBucket> getBuckets() {
    return Collections.unmodifiableCollection(bucketMap.values());
  }

  public boolean existsBucket(final String bucketName) {
    return bucketMap.containsKey(bucketName);
  }

  @Override
  public PBucket getBucketByName(final String name) {
    final PBucket p = bucketMap.get(name);
    if (p == null)
      throw new PSchemaException("Bucket with name '" + name + "' was not found");
    return p;
  }

  @Override
  public PBucket getBucketById(final int id) {
    if (id < 0 || id >= files.size())
      throw new PSchemaException("Bucket with id '" + id + "' was not found");

    final PPaginatedComponent p = files.get(id);
    if (p == null || !(p instanceof PBucket))
      throw new PSchemaException("Bucket with id '" + id + "' was not found");
    return (PBucket) p;
  }

  @Override
  public PBucket createBucket(final String bucketName) {
    return createBucket(bucketName, PBucket.DEF_PAGE_SIZE);
  }

  public PBucket createBucket(final String bucketName, final int pageSize) {
    return (PBucket) database.executeInWriteLock(new Callable<Object>() {
      @Override
      public Object call() throws Exception {
        if (bucketMap.containsKey(bucketName))
          throw new PSchemaException("Cannot create bucket '" + bucketName + "' because already exists");

        try {
          final PBucket bucket = new PBucket(database, bucketName, databasePath + "/" + bucketName, PPaginatedFile.MODE.READ_WRITE,
              pageSize);
          registerFile(bucket);
          bucketMap.put(bucketName, bucket);

          return bucket;

        } catch (IOException e) {
          throw new PSchemaException("Cannot create bucket '" + bucketName + "' (error=" + e + ")", e);
        }
      }
    });
  }

  @Override
  public boolean existsIndex(final String indexName) {
    return indexMap.containsKey(indexName);
  }

  @Override
  public PIndex[] getIndexes() {
    final PIndex[] indexes = new PIndex[indexMap.size()];
    int i = 0;
    for (PIndex index : indexMap.values())
      indexes[i++] = index;
    return indexes;
  }

  @Override
  public PIndex getIndexByName(final String indexName) {
    final PIndex p = indexMap.get(indexName);
    if (p == null)
      throw new PSchemaException("Index with name '" + indexName + "' was not found");
    return p;
  }

  @Override
  public PIndex[] createClassIndexes(final String typeName, final String[] propertyNames) {
    return createClassIndexes(typeName, propertyNames, PIndexLSM.DEF_PAGE_SIZE);
  }

  @Override
  public PIndex[] createClassIndexes(final String typeName, final String[] propertyNames, final int pageSize) {
    return createClassIndexes(typeName, propertyNames, pageSize, propertyNames.length);
  }

  public PIndex[] createClassIndexes(final String typeName, final String[] propertyNames, final int pageSize,
      final int bfKeyDepth) {
    return (PIndex[]) database.executeInWriteLock(new Callable<Object>() {
      @Override
      public Object call() throws Exception {
        try {
          if (propertyNames.length == 0)
            throw new PDatabaseMetadataException(
                "Cannot create index on type '" + typeName + "' because there are no property defined");

          final PDocumentType type = getType(typeName);

          // CHECK ALL THE PROPERTIES EXIST
          final byte[] keyTypes = new byte[propertyNames.length];
          int i = 0;

          for (String propertyName : propertyNames) {
            final PProperty property = type.getPolymorphicProperty(propertyName);
            if (property == null)
              throw new PSchemaException(
                  "Cannot create the index on type '" + typeName + "." + propertyName + "' because the property does not exist");

            keyTypes[i++] = PBinaryTypes.getTypeFromClass(property.getType());
          }

          final List<PBucket> buckets = type.getBuckets(false);

          final PIndexLSM[] indexes = new PIndexLSM[buckets.size()];
          for (int idx = 0; idx < buckets.size(); ++idx) {
            final PBucket b = buckets.get(idx);
            final String indexName = b.getName() + "_" + System.currentTimeMillis();

            if (indexMap.containsKey(indexName))
              throw new PDatabaseMetadataException(
                  "Cannot create index '" + indexName + "' on type '" + typeName + "' because it already exists");

            indexes[idx] = new PIndexLSM(database, indexName, databasePath + "/" + indexName, PPaginatedFile.MODE.READ_WRITE,
                keyTypes, PBinaryTypes.TYPE_RID, pageSize, bfKeyDepth);

            registerFile(indexes[idx]);
            indexMap.put(indexName, indexes[idx]);

            type.addIndexInternal(indexes[idx], b, propertyNames);
          }

          saveConfiguration();

          return indexes;

        } catch (IOException e) {
          throw new PSchemaException("Cannot create index on type '" + typeName + "' (error=" + e + ")", e);
        }
      }
    });
  }

  public PIndex createManualIndex(final String indexName, final byte[] keyTypes, final int pageSize) {
    return createManualIndex(indexName, keyTypes, pageSize, keyTypes.length);
  }

  public PIndex createManualIndex(final String indexName, final byte[] keyTypes, final int pageSize, final int bfKeyDepth) {
    return (PIndexLSM) database.executeInWriteLock(new Callable<Object>() {
      @Override
      public Object call() throws Exception {
        if (indexMap.containsKey(indexName))
          throw new PSchemaException("Cannot create index '" + indexName + "' because already exists");

        try {
          final PIndexLSM index = new PIndexLSM(database, indexName, databasePath + "/" + indexName, PPaginatedFile.MODE.READ_WRITE,
              keyTypes, PBinaryTypes.TYPE_RID, pageSize, bfKeyDepth);
          registerFile(index);
          indexMap.put(indexName, index);

          return index;

        } catch (IOException e) {
          throw new PSchemaException("Cannot create index '" + indexName + "' (error=" + e + ")", e);
        }
      }
    });
  }

  public void close() {
    files.clear();
    types.clear();
    bucketMap.clear();
    indexMap.clear();
    dictionary = null;
  }

  public PDictionary getDictionary() {
    return dictionary;
  }

  public PDatabase getDatabase() {
    return database;
  }

  public Collection<PDocumentType> getTypes() {
    return Collections.unmodifiableCollection(types.values());
  }

  public PDocumentType getType(final String typeName) {
    final PDocumentType t = types.get(typeName);
    if (t == null)
      throw new PSchemaException("Type with name '" + typeName + "' was not found");
    return t;
  }

  @Override
  public String getTypeNameByBucketId(final int bucketId) {
    for (PDocumentType t : types.values()) {
      for (PBucket b : t.getBuckets(false)) {
        if (b.getId() == bucketId)
          return t.getName();
      }
    }

    // NOT FOUND
    return null;
  }

  @Override
  public PDocumentType getTypeByBucketId(final int bucketId) {
    for (PDocumentType t : types.values()) {
      for (PBucket b : t.getBuckets(false)) {
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

  public PDocumentType createDocumentType(final String typeName) {
    return createDocumentType(typeName, 1);
  }

  public PDocumentType createDocumentType(final String typeName, final int buckets) {
    return createDocumentType(typeName, buckets, PBucket.DEF_PAGE_SIZE);
  }

  public PDocumentType createDocumentType(final String typeName, final int buckets, final int pageSize) {
    return (PDocumentType) database.executeInWriteLock(new Callable<Object>() {
      @Override
      public Object call() throws Exception {
        if (typeName.indexOf(",") > -1)
          throw new IllegalArgumentException("Type name '" + typeName + "' contains non valid characters");

        if (types.containsKey(typeName))
          throw new PSchemaException("Type '" + typeName + "' already exists");
        final PDocumentType c = new PDocumentType(PSchemaImpl.this, typeName);
        types.put(typeName, c);

        for (int i = 0; i < buckets; ++i)
          c.addBucket(createBucket(typeName + "_" + i, pageSize));

        saveConfiguration();

        return c;
      }
    });
  }

  @Override
  public PVertexType createVertexType(final String typeName) {
    return createVertexType(typeName, 1);
  }

  @Override
  public PVertexType createVertexType(final String typeName, final int buckets) {
    return createVertexType(typeName, buckets, PBucket.DEF_PAGE_SIZE);
  }

  @Override
  public PVertexType createVertexType(String typeName, final int buckets, final int pageSize) {
    return (PVertexType) database.executeInWriteLock(new Callable<Object>() {
      @Override
      public Object call() throws Exception {
        if (typeName.indexOf(",") > -1)
          throw new IllegalArgumentException("Vertex type name '" + typeName + "' contains non valid characters");

        if (types.containsKey(typeName))
          throw new PSchemaException("Vertex type '" + typeName + "' already exists");
        final PDocumentType c = new PVertexType(PSchemaImpl.this, typeName);
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
  public PEdgeType createEdgeType(final String typeName) {
    return createEdgeType(typeName, 1);
  }

  @Override
  public PEdgeType createEdgeType(final String typeName, final int buckets) {
    return createEdgeType(typeName, buckets, PBucket.DEF_PAGE_SIZE);
  }

  @Override
  public PEdgeType createEdgeType(final String typeName, final int buckets, final int pageSize) {
    return (PEdgeType) database.executeInWriteLock(new Callable<Object>() {
      @Override
      public Object call() throws Exception {
        if (typeName.indexOf(",") > -1)
          throw new IllegalArgumentException("Edge type name '" + typeName + "' contains non valid characters");

        if (types.containsKey(typeName))
          throw new PSchemaException("Edge type '" + typeName + "' already exists");
        final PDocumentType c = new PEdgeType(PSchemaImpl.this, typeName);
        types.put(typeName, c);

        for (int i = 0; i < buckets; ++i)
          c.addBucket(createBucket(typeName + "_" + i, pageSize));

        saveConfiguration();

        return c;
      }
    });
  }

  public void swapIndexes(final PIndexLSM oldIndex, final PIndexLSM newIndex) throws IOException {
    indexMap.remove(oldIndex.getName());

    indexMap.put(newIndex.getName(), newIndex);

    // SCAN ALL THE TYPES TO FIND WHERE THE INDEX WAS DEFINED TO REPLACE IT
    for (PDocumentType t : getTypes()) {
      for (List<PDocumentType.IndexMetadata> metadata : t.getAllIndexesMetadata()) {
        for (PDocumentType.IndexMetadata m : metadata) {
          if (m.index.equals(oldIndex)) {
            m.index = newIndex;
            break;
          }
        }
      }
    }

    newIndex.removeTempSuffix();

    oldIndex.drop();
  }

  protected void readConfiguration() throws IOException {
    types.clear();

    final File file = new File(databasePath + SCHEMA_FILE_NAME);
    if (!file.exists())
      return;

    final String fileContent = PFileUtils.readStreamAsString(new FileInputStream(file), encoding);

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

      final PDocumentType type;

      final String kind = (String) schemaType.get("type");
      if ("v".equals(kind)) {
        type = new PVertexType(this, typeName);
      } else if ("e".equals(kind)) {
        type = new PEdgeType(this, typeName);
      } else if ("d".equals(kind)) {
        type = new PDocumentType(this, typeName);
      } else
        throw new PConfigurationException("Type '" + kind + "' is not supported");

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
          final PPaginatedComponent bucket = bucketMap.get(schemaBucket.getString(i));
          if (bucket == null || !(bucket instanceof PBucket))
            PLogManager.instance().warn(this, "Cannot find bucket %s for type '%s'", schemaBucket.getInt(i), type);
          type.addBucketInternal((PBucket) bucket);
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
      final PDocumentType type = getType(entry.getKey());
      for (String p : entry.getValue())
        type.addParent(getType(p));
    }
  }

  protected void saveConfiguration() {
    try {
      final FileWriter file = new FileWriter(databasePath + SCHEMA_FILE_NAME);

      final JSONObject root = new JSONObject();
      root.put("version", PConstants.VERSION);

      final JSONObject settings = new JSONObject();
      root.put("settings", settings);

      settings.put("timeZone", timeZone.getDisplayName());
      settings.put("dateFormat", dateFormat);
      settings.put("dateTimeFormat", dateTimeFormat);

      final JSONObject types = new JSONObject();
      root.put("types", types);

      for (PDocumentType t : this.types.values()) {
        final JSONObject type = new JSONObject();
        types.put(t.getName(), type);

        final String kind;
        if (t instanceof PVertexType)
          kind = "v";
        else if (t instanceof PEdgeType)
          kind = "e";
        else
          kind = "d";
        type.put("type", kind);

        final String[] parents = new String[t.getParentTypes().size()];
        for (int i = 0; i < parents.length; ++i)
          parents[i] = t.getParentTypes().get(i).getName();
        type.put("parents", parents);

        final List<PBucket> originalBuckets = t.getBuckets(false);
        final String[] buckets = new String[originalBuckets.size()];
        for (int i = 0; i < buckets.length; ++i)
          buckets[i] = originalBuckets.get(i).getName();

        type.put("buckets", buckets);
        type.put("syncSelectionStrategy", t.getSyncSelectionStrategy().getName());
        type.put("asyncSelectionStrategy", t.getAsyncSelectionStrategy().getName());

        final JSONObject indexes = new JSONObject();
        type.put("indexes", indexes);

        for (List<PDocumentType.IndexMetadata> list : t.getAllIndexesMetadata()) {
          for (PDocumentType.IndexMetadata entry : list) {
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
      PLogManager.instance().error(this, "Error on saving schema configuration to file: %s", e, databasePath + SCHEMA_FILE_NAME);
    }
  }

  public void registerFile(final PPaginatedComponent file) {
    while (files.size() < file.getId() + 1)
      files.add(null);

    if (files.get(file.getId()) != null)
      throw new PSchemaException("File with id '" + file.getId() + "' already exists (" + files.get(file.getId()) + ")");

    files.set(file.getId(), file);
  }
}
