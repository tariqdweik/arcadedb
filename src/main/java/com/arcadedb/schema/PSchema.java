package com.arcadedb.schema;

import com.arcadedb.engine.PBucket;
import com.arcadedb.engine.PDictionary;
import com.arcadedb.engine.PIndex;
import com.arcadedb.engine.PPaginatedFile;

import java.util.Collection;

public interface PSchema {

  PPaginatedFile getFileById(int id);

  void removeFile(int fileId);

  boolean existsBucket(String bucketName);

  PBucket getBucketByName(String name);

  PBucket getBucketById(int id);

  PBucket createBucket(String bucketName);

  boolean existsIndex(String indexName);

  PIndex[] getIndexes();

  PIndex getIndexByName(String indexName);

  PIndex[] createClassIndexes(String typeName, String[] propertyNames);

  PIndex[] createClassIndexes(String typeName, String[] propertyNames, int pageSize);

  PIndex createManualIndex(String indexName, byte[] keyTypes, int pageSize);

  PDictionary getDictionary();

  Collection<PType> getTypes();

  PType getType(String typeName);

  boolean existsType(String typeName);

  PType createType(String typeName);

  PType createType(String typeName, int buckets);
}
