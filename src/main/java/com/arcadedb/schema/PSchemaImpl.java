package com.arcadedb.schema;

import com.arcadedb.PConstants;
import com.arcadedb.database.PDatabase;
import com.arcadedb.engine.*;
import com.arcadedb.exception.PConfigurationException;
import com.arcadedb.exception.PDatabaseMetadataException;
import com.arcadedb.exception.PSchemaException;
import com.arcadedb.exception.PTransactionException;
import com.arcadedb.serializer.PBinaryTypes;
import com.arcadedb.utility.PLogManager;

import java.io.*;
import java.util.*;
import java.util.concurrent.Callable;

public class PSchemaImpl implements PSchema {
  private static final String SCHEMA_FILE_NAME = "/schema.pcsv";
  private final PDatabase database;
  private final List<PPaginatedFile> files     = new ArrayList<PPaginatedFile>();
  private final Map<String, PType>   types     = new HashMap<String, PType>();
  private final Map<String, PBucket> bucketMap = new HashMap<String, PBucket>();
  private final Map<String, PIndex>  indexMap  = new HashMap<String, PIndex>();
  private final String      databasePath;
  private       PDictionary dictionary;

  public PSchemaImpl(final PDatabase database, final String databasePath, final PFile.MODE mode) {
    this.database = database;
    this.databasePath = databasePath;

    if (database.getFileManager().getFiles().isEmpty())
      create(mode);
    else
      load(mode);
  }

  private void create(final PFile.MODE mode) {
    try {
      dictionary = new PDictionary(database, "dictionary", databasePath + "/dictionary", mode);
      files.add(dictionary);

    } catch (IOException e) {
      PLogManager.instance().error(this, "Error on opening dictionary '%s' (error=%s)", e, databasePath, e.toString());
    }
  }

  private void load(final PFile.MODE mode) {
    for (PFile file : database.getFileManager().getFiles()) {
      final String fileName = file.getFileName();
      final int fileId = file.getFileId();
      final String fileExt = file.getFileExtension();

      PPaginatedFile pf = null;

      if (fileExt.equals(PDictionary.DICT_EXT)) {
        // DICTIONARY
        try {
          dictionary = new PDictionary(database, fileName, file.getFilePath(), fileId, mode);
          pf = dictionary;

        } catch (IOException e) {
          PLogManager.instance().error(this, "Error on opening dictionary '%s' (error=%s)", e, file, e.toString());
        }

      } else if (fileExt.equals(PBucket.BUCKET_EXT)) {
        // BUCKET
        try {
          final PBucket newPart = new PBucket(database, fileName, file.getFilePath(), fileId, mode);
          bucketMap.put(fileName, newPart);
          pf = newPart;
        } catch (IOException e) {
          PLogManager.instance().error(this, "Error on opening bucket '%s' (error=%s)", e, file, e.toString());
        }

      } else if (fileExt.equals(PIndex.INDEX_EXT)) {
        // INDEX
        try {
          final PIndex index = new PIndex(database, fileName, file.getFilePath(), fileId, mode);
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
  public PPaginatedFile getFileById(final int id) {
    if (id >= files.size())
      throw new PSchemaException("File with id '" + id + "' was not found");

    final PPaginatedFile p = files.get(id);
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

  public boolean existsBucket(final String bucketName) {
    return bucketMap.containsKey(bucketName);
  }

  public PBucket getBucketByName(final String name) {
    final PBucket p = bucketMap.get(name);
    if (p == null)
      throw new PSchemaException("Bucket with name '" + name + "' was not found");
    return p;
  }

  public PBucket getBucketById(final int id) {
    final PPaginatedFile p = files.get(id);
    if (p == null || !(p instanceof PBucket))
      throw new PSchemaException("Bucket with id '" + id + "' was not found");
    return (PBucket) p;
  }

  public PBucket createBucket(final String bucketName) {
    return (PBucket) database.executeInLock(new Callable<Object>() {
      @Override
      public Object call() throws Exception {
        if (bucketMap.containsKey(bucketName))
          throw new PSchemaException("Cannot create bucket '" + bucketName + "' because already exists");

        try {
          final PBucket bucket = new PBucket(database, bucketName, databasePath + "/" + bucketName, PFile.MODE.READ_WRITE);
          registerFile(bucket);
          bucketMap.put(bucketName, bucket);

          return bucket;

        } catch (IOException e) {
          throw new PSchemaException("Cannot create bucket '" + bucketName + "' (error=" + e + ")", e);
        }
      }
    });
  }

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

  public PIndex getIndexByName(final String indexName) {
    final PIndex p = indexMap.get(indexName);
    if (p == null)
      throw new PSchemaException("Index with name '" + indexName + "' was not found");
    return p;
  }

  public PIndex[] createClassIndexes(final String typeName, final String... propertyNames) {
    return (PIndex[]) database.executeInLock(new Callable<Object>() {
      @Override
      public Object call() throws Exception {
        try {
          if (propertyNames.length == 0)
            throw new PDatabaseMetadataException(
                "Cannot create index on type '" + typeName + "' because there are no property defined");

          final PType type = getType(typeName);
          if (type == null)
            throw new PSchemaException("Cannot create the index on type '" + typeName + "." + Arrays.toString(propertyNames)
                + "' because the type does not exist");

          // CHECK ALL THE PROPERTIES EXIST
          final byte[] keyTypes = new byte[propertyNames.length];
          int i = 0;

          for (String propertyName : propertyNames) {
            final PProperty property = type.getProperty(propertyName);
            if (property == null)
              throw new PSchemaException(
                  "Cannot create the index on type '" + typeName + "." + propertyName + "' because the property does not exist");

            keyTypes[i++] = PBinaryTypes.getTypeFromClass(property.getType());
          }

          final PIndex[] indexes = new PIndex[type.getBuckets().size()];
          for (int idx = 0; idx < type.getBuckets().size(); ++idx) {
            final PBucket b = type.getBuckets().get(idx);
            final String indexName = b.getName() + "_" + System.currentTimeMillis();

            if (indexMap.containsKey(indexName))
              throw new PDatabaseMetadataException(
                  "Cannot create index '" + indexName + "' on type '" + typeName + "' because it already exists");

            indexes[idx] = new PIndex(database, indexName, databasePath + "/" + indexName, PFile.MODE.READ_WRITE, keyTypes,
                PBinaryTypes.TYPE_RID);

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

  public PIndex createManualIndex(final String indexName, final byte[] keyTypes) {
    return (PIndex) database.executeInLock(new Callable<Object>() {
      @Override
      public Object call() throws Exception {
        if (database.getTransaction().getModifiedPages() > 0)
          throw new PTransactionException("Cannot create a new index in the middle of a transaction");

        if (indexMap.containsKey(indexName))
          throw new PSchemaException("Cannot create index '" + indexName + "' because already exists");

        try {
          final PIndex index = new PIndex(database, indexName, databasePath + "/" + indexName, PFile.MODE.READ_WRITE, keyTypes,
              PBinaryTypes.TYPE_RID);
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

  public Collection<PType> getTypes() {
    return Collections.unmodifiableCollection(types.values());
  }

  public PType getType(final String typeName) {
    return types.get(typeName);
  }

  public boolean existsType(final String typeName) {
    return types.containsKey(typeName);
  }

  public PType createType(final String typeName) {
    return createType(typeName, 1);
  }

  public PType createType(final String typeName, final int buckets) {
    return (PType) database.executeInLock(new Callable<Object>() {
      @Override
      public Object call() throws Exception {
        if (typeName.indexOf(",") > -1)
          throw new IllegalArgumentException("Type name '" + typeName + "' contains non valid characters");

        if (types.containsKey(typeName))
          throw new PSchemaException("Type '" + typeName + "' already exists");
        final PType c = new PType(PSchemaImpl.this, typeName);
        types.put(typeName, c);

        for (int i = 0; i < buckets; ++i)
          c.addBucket(createBucket(typeName + "_" + i));

        saveConfiguration();

        return c;
      }
    });
  }

  public void swapIndexes(final PIndex oldIndex, final PIndex newIndex) throws IOException {
    indexMap.put(oldIndex.getName(), newIndex);

    // SCAN ALL THE TYPES TO FIND WHERE THE INDEX WAS DEFINED TO REPLACE IT
    for (PType t : getTypes()) {
      for (List<PType.IndexMetadata> metadata : t.getAllIndexesMetadata()) {
        for (PType.IndexMetadata m : metadata) {
          if (m.index.equals(oldIndex)) {
            m.index = newIndex;
            break;
          }
        }
      }
    }

    oldIndex.drop();
  }

  protected void readConfiguration() {
    types.clear();

    final File file = new File(databasePath + SCHEMA_FILE_NAME);
    if (!file.exists())
      return;

    try (BufferedReader br = new BufferedReader(new FileReader(file))) {
      int lineNum = 0;
      String line;

      PType lastType = null;

      while ((line = br.readLine()) != null) {
        if (lineNum == 0) {
          if (!line.startsWith("#PROTON,"))
            throw new PConfigurationException("Format exception while parsing schema file: " + (databasePath + SCHEMA_FILE_NAME));
        } else if (!line.startsWith("+")) {
          // TYPE
          final String[] parts = line.split(",");

          final PType type = new PType(this, parts[0]);
          // parts[1] // IGNORE STRATEGY FOR NOW
          final String[] bucketItems = parts[2].split(";");
          for (String b : bucketItems) {
            final PPaginatedFile bucket = files.get(Integer.parseInt(b));
            if (bucket == null || !(bucket instanceof PBucket))
              PLogManager.instance().warn(this, "Cannot find bucket %s for type '%s'", b, parts[0]);
            type.addBucketInternal((PBucket) bucket);
          }

          types.put(type.getName(), type);
          lastType = type;
        } else {
          // INDEX
          final String[] parts = line.substring(1).split(",");
          final String bucketName = parts[1];
          final String[] keys = parts[2].split(";");
          lastType.addIndexInternal(getIndexByName(parts[0]), bucketMap.get(bucketName), keys);
        }
        lineNum++;
      }

    } catch (IOException e) {
      PLogManager.instance().error(this, "Error on loading schema configuration from file: %s", e, databasePath + SCHEMA_FILE_NAME);
    }
  }

  protected void saveConfiguration() {
    try {
      final FileWriter file = new FileWriter(databasePath + SCHEMA_FILE_NAME);

      file.append(String.format("#PROTON,%s\n", PConstants.VERSION));
      for (PType t : types.values()) {
        final StringBuilder bucketList2String = new StringBuilder();
        for (PBucket b : t.getBuckets()) {
          if (bucketList2String.length() > 0)
            bucketList2String.append(';');
          bucketList2String.append(b.getId());
        }

        file.append(String.format("%s,%s,%s\n", t.getName(), t.getSelectionStrategy().getName(), bucketList2String.toString()));

        for (List<PType.IndexMetadata> list : t.getAllIndexesMetadata()) {
          for (PType.IndexMetadata entry : list) {
            final StringBuilder keys2String = new StringBuilder();
            for (String k : entry.propertyNames) {
              if (keys2String.length() > 0)
                keys2String.append(';');
              keys2String.append(k);
            }

            file.append(String
                .format("+%s,%s,%s\n", entry.index.getName(), getBucketById(entry.bucketId).getName(), keys2String.toString()));
          }
        }
      }
      file.close();

    } catch (IOException e) {
      PLogManager.instance().error(this, "Error on saving schema configuration to file: %s", e, databasePath + SCHEMA_FILE_NAME);
    }
  }

  public void registerFile(final PPaginatedFile file) {
    while (files.size() < file.getId() + 1)
      files.add(null);

    if (files.get(file.getId()) != null)
      throw new PSchemaException("File with id '" + file.getId() + "' already exists (" + files.get(file.getId()) + ")");

    files.set(file.getId(), file);
  }
}
