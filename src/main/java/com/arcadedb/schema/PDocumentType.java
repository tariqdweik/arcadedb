package com.arcadedb.schema;

import com.arcadedb.database.PBucketSelectionStrategy;
import com.arcadedb.database.PDocument;
import com.arcadedb.database.PRoundRobinBucketSelectionStrategy;
import com.arcadedb.database.PThreadAffinityBucketSelectionStrategy;
import com.arcadedb.engine.PBucket;
import com.arcadedb.exception.PSchemaException;
import com.arcadedb.index.PIndex;

import java.util.*;

public class PDocumentType {
  private final PSchemaImpl                            schema;
  private final String                                 name;
  private final List<PBucket>                          buckets                = new ArrayList<PBucket>();
  private       PBucketSelectionStrategy               syncSelectionStrategy  = new PThreadAffinityBucketSelectionStrategy();
  private       PBucketSelectionStrategy               asyncSelectionStrategy = new PRoundRobinBucketSelectionStrategy();
  private final Map<String, PProperty>                 properties             = new HashMap<>();
  private       Map<Integer, List<IndexMetadata>>      indexesByBucket        = new HashMap<>();
  private       Map<List<String>, List<IndexMetadata>> indexesByProperties    = new HashMap<>();

  public class IndexMetadata {
    public String[] propertyNames;
    public int      bucketId;
    public PIndex   index;

    public IndexMetadata(final PIndex index, final int bucketId, final String[] propertyNames) {
      this.index = index;
      this.bucketId = bucketId;
      this.propertyNames = propertyNames;
    }
  }

  public PDocumentType(final PSchemaImpl schema, final String name) {
    this.schema = schema;
    this.name = name;
  }

  public String getName() {
    return name;
  }

  public byte getType() {
    return PDocument.RECORD_TYPE;
  }

  public Set<String> getPropertyNames() {
    return properties.keySet();
  }

  public PProperty createProperty(final String propertyName, final Class<?> propertyType) {
    if (properties.containsKey(propertyName))
      throw new PSchemaException(
          "Cannot create the property '" + propertyName + "' in type '" + name + "' because it already exists");

    final PProperty property = new PProperty(this, propertyName, propertyType);

    properties.put(propertyName, property);

    return property;
  }

  public List<PBucket> getBuckets() {
    return buckets;
  }

  private boolean hasBucket(final String bucketName) {
    for (PBucket b : buckets)
      if (b.getName().equals(bucketName))
        return true;
    return false;
  }

  public void addBucket(final PBucket bucket) {
    addBucketInternal(bucket);
    schema.saveConfiguration();
  }

  public PBucket getBucketToSave(final boolean async) {
    if (buckets.isEmpty())
      throw new PSchemaException("Cannot save on a bucket for type '" + name + "' because there are no buckets associated");
    return buckets.get(async ? asyncSelectionStrategy.getBucketToSave() : syncSelectionStrategy.getBucketToSave());
  }

  public PBucketSelectionStrategy getSyncSelectionStrategy() {
    return syncSelectionStrategy;
  }

  public void setSyncSelectionStrategy(final PBucketSelectionStrategy selectionStrategy) {
    this.syncSelectionStrategy = selectionStrategy;
  }

  public PBucketSelectionStrategy getAsyncSelectionStrategy() {
    return asyncSelectionStrategy;
  }

  public void setAsyncSelectionStrategy(final PBucketSelectionStrategy selectionStrategy) {
    this.asyncSelectionStrategy = selectionStrategy;
  }

  public boolean existsProperty(final String propertyName) {
    return properties.containsKey(propertyName);
  }

  public PProperty getProperty(final String propertyName) {
    final PProperty prop = properties.get(propertyName);
    if (prop == null)
      throw new PSchemaException("Cannot find property '" + propertyName + "' in type '" + name + "'");
    return prop;
  }

  public Collection<List<IndexMetadata>> getAllIndexesMetadata() {
    return indexesByBucket.values();
  }

  public List<IndexMetadata> getIndexMetadataByBucketId(final int bucketId) {
    return indexesByBucket.get(bucketId);
  }

  public List<IndexMetadata> getIndexMetadataByProperties(final String... properties) {
    return indexesByProperties.get(Arrays.asList(properties));
  }

  public PSchema getSchema() {
    return schema;
  }

  @Override
  public String toString() {
    return name;
  }

  protected void addIndexInternal(final PIndex index, final PBucket bucket, final String[] propertyNames) {
    final IndexMetadata metadata = new IndexMetadata(index, bucket.getId(), propertyNames);

    List<IndexMetadata> list1 = indexesByBucket.get(bucket.getId());
    if (list1 == null) {
      list1 = new ArrayList<>();
      indexesByBucket.put(bucket.getId(), list1);
    }
    list1.add(metadata);

    final List<String> propertyList = Arrays.asList(propertyNames);

    List<IndexMetadata> list2 = indexesByProperties.get(propertyList);
    if (list2 == null) {
      list2 = new ArrayList<>();
      indexesByProperties.put(propertyList, list2);
    }
    list2.add(metadata);
  }

  protected void addBucketInternal(final PBucket bucket) {
    for (PDocumentType cl : schema.getTypes()) {
      if (cl.hasBucket(bucket.getName()))
        throw new PSchemaException("Cannot add the bucket '" + bucket.getName() + "' to the type '" + name
            + "', because the bucket is already associated to the type '" + cl.getName() + "'");
    }

    buckets.add(bucket);
    syncSelectionStrategy.setTotalBuckets(buckets.size());
    asyncSelectionStrategy.setTotalBuckets(buckets.size());
  }
}
