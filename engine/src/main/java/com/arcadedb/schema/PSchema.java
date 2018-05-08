package com.arcadedb.schema;

import com.arcadedb.engine.Bucket;
import com.arcadedb.engine.Dictionary;
import com.arcadedb.engine.PaginatedComponent;
import com.arcadedb.index.PIndex;

import java.util.Collection;
import java.util.TimeZone;

public interface PSchema {

  void setDateTimeFormat(String dateTimeFormat);

  PaginatedComponent getFileById(int id);

  void removeFile(int fileId);

  boolean existsBucket(String bucketName);

  Bucket getBucketByName(String name);

  Collection<Bucket> getBuckets();

  Bucket getBucketById(int id);

  Bucket createBucket(String bucketName);

  boolean existsIndex(String indexName);

  PIndex[] getIndexes();

  PIndex getIndexByName(String indexName);

  PIndex[] createClassIndexes(String typeName, String[] propertyNames);

  PIndex[] createClassIndexes(String typeName, String[] propertyNames, int pageSize);

  PIndex createManualIndex(String indexName, byte[] keyTypes, int pageSize);

  Dictionary getDictionary();

  Collection<PDocumentType> getTypes();

  PDocumentType getType(String typeName);

  String getTypeNameByBucketId(int bucketId);

  PDocumentType getTypeByBucketId(int bucketId);

  boolean existsType(String typeName);

  PDocumentType createDocumentType(String typeName);

  PDocumentType createDocumentType(String typeName, int buckets);

  PDocumentType createDocumentType(String typeName, int buckets, int pageSize);

  PVertexType createVertexType(String typeName);

  PVertexType createVertexType(String typeName, int buckets);

  PVertexType createVertexType(String typeName, int buckets, int pageSize);

  PEdgeType createEdgeType(String typeName);

  PEdgeType createEdgeType(String typeName, int buckets);

  PEdgeType createEdgeType(String typeName, int buckets, int pageSize);

  TimeZone getTimeZone();

  void setTimeZone(TimeZone timeZone);

  String getDateFormat();

  void setDateFormat(String dateFormat);

  String getDateTimeFormat();
}
