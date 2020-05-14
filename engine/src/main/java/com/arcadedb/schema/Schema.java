/*
 * Copyright (c) - Arcade Data LTD (https://arcadedata.com)
 */

package com.arcadedb.schema;

import com.arcadedb.engine.Bucket;
import com.arcadedb.engine.Dictionary;
import com.arcadedb.engine.PaginatedComponent;
import com.arcadedb.index.Index;
import com.arcadedb.index.TypeIndex;
import com.arcadedb.index.lsm.LSMTreeIndexAbstract;

import java.util.Collection;
import java.util.TimeZone;

public interface Schema {

  PaginatedComponent getFileById(int id);

  boolean existsBucket(String bucketName);

  Bucket getBucketByName(String name);

  PaginatedComponent getFileByIdIfExists(int id);

  Collection<Bucket> getBuckets();

  Bucket getBucketById(int id);

  Bucket createBucket(String bucketName);

  boolean existsIndex(String indexName);

  DocumentType copyType(String typeName, String newTypeName, Class<? extends DocumentType> newType, int buckets, int pageSize, int transactionBatchSize);

  Index[] getIndexes();

  Index getIndexByName(String indexName);

  TypeIndex createTypeIndex(SchemaImpl.INDEX_TYPE indexType, boolean unique, String typeName, String... propertyNames);

  TypeIndex createTypeIndex(SchemaImpl.INDEX_TYPE indexType, boolean unique, String typeName, String[] propertyNames, int pageSize);

  TypeIndex createTypeIndex(SchemaImpl.INDEX_TYPE indexType, boolean unique, String typeName, String[] propertyNames, int pageSize,
      Index.BuildIndexCallback callback);

  TypeIndex createTypeIndex(SchemaImpl.INDEX_TYPE indexType, boolean unique, String typeName, String[] propertyNames, int pageSize,
      LSMTreeIndexAbstract.NULL_STRATEGY nullStrategy, Index.BuildIndexCallback callback);

  TypeIndex getOrCreateTypeIndex(SchemaImpl.INDEX_TYPE indexType, boolean unique, String typeName, String... propertyNames);

  TypeIndex getOrCreateTypeIndex(SchemaImpl.INDEX_TYPE indexType, boolean unique, String typeName, String[] propertyNames, int pageSize);

  TypeIndex getOrCreateTypeIndex(SchemaImpl.INDEX_TYPE indexType, boolean unique, String typeName, String[] propertyNames, int pageSize,
      Index.BuildIndexCallback callback);

  TypeIndex getOrCreateTypeIndex(SchemaImpl.INDEX_TYPE indexType, boolean unique, String typeName, String[] propertyNames, int pageSize,
      LSMTreeIndexAbstract.NULL_STRATEGY nullStrategy, Index.BuildIndexCallback callback);

  Index createBucketIndex(SchemaImpl.INDEX_TYPE indexType, boolean unique, String typeName, String bucketName, String[] propertyNames, int pageSize,
      LSMTreeIndexAbstract.NULL_STRATEGY nullStrategy, Index.BuildIndexCallback callback);

  Index createManualIndex(SchemaImpl.INDEX_TYPE indexType, boolean unique, String indexName, byte[] keyTypes, int pageSize,
      LSMTreeIndexAbstract.NULL_STRATEGY nullStrategy);

  Dictionary getDictionary();

  Collection<DocumentType> getTypes();

  DocumentType getType(String typeName);

  void dropType(String typeName);

  String getTypeNameByBucketId(int bucketId);

  DocumentType getTypeByBucketId(int bucketId);

  boolean existsType(String typeName);

  void dropBucket(String bucketName);

  void dropIndex(String indexName);

  DocumentType createDocumentType(String typeName);

  DocumentType createDocumentType(String typeName, int buckets);

  DocumentType createDocumentType(String typeName, int buckets, int pageSize);

  DocumentType getOrCreateDocumentType(String typeName);

  DocumentType getOrCreateDocumentType(String typeName, int buckets);

  DocumentType getOrCreateDocumentType(String typeName, int buckets, int pageSize);

  VertexType createVertexType(String typeName);

  VertexType createVertexType(String typeName, int buckets);

  VertexType createVertexType(String typeName, int buckets, int pageSize);

  VertexType getOrCreateVertexType(String typeName);

  VertexType getOrCreateVertexType(String typeName, int buckets);

  VertexType getOrCreateVertexType(String typeName, int buckets, int pageSize);

  EdgeType createEdgeType(String typeName);

  EdgeType createEdgeType(String typeName, int buckets);

  EdgeType createEdgeType(String typeName, int buckets, int pageSize);

  EdgeType getOrCreateEdgeType(String typeName);

  EdgeType getOrCreateEdgeType(String typeName, int buckets);

  EdgeType getOrCreateEdgeType(String typeName, int buckets, int pageSize);

  TimeZone getTimeZone();

  void setTimeZone(TimeZone timeZone);

  String getDateFormat();

  void setDateFormat(String dateFormat);

  String getDateTimeFormat();

  void setDateTimeFormat(String dateTimeFormat);

  String getEncoding();

}
