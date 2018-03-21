package com.arcadedb.schema;

import com.arcadedb.engine.PBucket;
import com.arcadedb.engine.PDictionary;
import com.arcadedb.index.PIndex;
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

  Collection<PDocumentType> getTypes();

  PDocumentType getType(String typeName);

  String getTypeNameByBucketId(int bucketId);

  PDocumentType getTypeByBucketId(int bucketId);

  boolean existsType(String typeName);

  PDocumentType createDocumentType(String typeName);

  PDocumentType createDocumentType(String typeName, int buckets);

  PDocumentType createDocumentType(String typeName, int buckets, int pageSize);

  PDocumentType createVertexType(String typeName);

  PDocumentType createVertexType(String typeName, int buckets);

  PDocumentType createVertexType(String typeName, int buckets, int pageSize);

  PDocumentType createEdgeType(String typeName);

  PDocumentType createEdgeType(String typeName, int buckets);

  PDocumentType createEdgeType(String typeName, int buckets, int pageSize);
}
