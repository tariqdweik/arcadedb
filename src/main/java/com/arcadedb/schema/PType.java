package com.arcadedb.schema;

import com.arcadedb.database.PBucketSelectionStrategy;
import com.arcadedb.database.PRoundRobinBucketSelectionStrategy;
import com.arcadedb.engine.PBucket;
import com.arcadedb.engine.PIndex;
import com.arcadedb.exception.PSchemaException;

import java.util.*;
import java.util.concurrent.Callable;

public class PType {
  private final PSchemaImpl schema;
  private final String      name;
  private final List<PBucket>                     buckets           = new ArrayList<PBucket>();
  private       PBucketSelectionStrategy          selectionStrategy = new PRoundRobinBucketSelectionStrategy();
  private final Map<String, PProperty>            properties        = new HashMap<>();
  private       Map<Integer, List<IndexMetadata>> indexes           = new HashMap<>();

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

  public PType(final PSchemaImpl schema, final String name) {
    this.schema = schema;
    this.name = name;
  }

  public String getName() {
    return name;
  }

  public Set<String> getPropertyNames() {
    return properties.keySet();
  }

  public PProperty createProperty(final String propertyName, final Class<?> propertyType) {
    if (properties.containsKey(propertyName))
      throw new PSchemaException(
          "Cannot create the property '" + propertyName + "' in class '" + name + "' because it already exists");

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
    schema.getDatabase().executeInLock(new Callable<Object>() {
      @Override
      public Object call() throws Exception {
        addBucketInternal(bucket);
        schema.saveConfiguration();
        return null;
      }
    });
  }

  public PBucket getBucketToSave() {
    if (buckets.isEmpty())
      throw new PSchemaException("Cannot save on a bucket for class '" + name + "' because there are no buckets associated");
    return buckets.get(selectionStrategy.getBucketToSave());
  }

  public PBucketSelectionStrategy getSelectionStrategy() {
    return selectionStrategy;
  }

  public void setSelectionStrategy(final PBucketSelectionStrategy selectionStrategy) {
    this.selectionStrategy = selectionStrategy;
  }

  public PProperty getProperty(final String propertyName) {
    return properties.get(propertyName);
  }

  public Collection<List<IndexMetadata>> getAllIndexesMetadata() {
    return indexes.values();
  }

  public List<IndexMetadata> getIndexMetadataByBucketId(final int bucketId) {
    return indexes.get(bucketId);
  }

  protected void addIndexInternal(final PIndex index, final PBucket bucket, final String[] propertyNames) {
    List<IndexMetadata> list = indexes.get(bucket.getId());
    if (list == null) {
      list = new ArrayList<>();
      indexes.put(bucket.getId(), list);
    }
    list.add(new IndexMetadata(index, bucket.getId(), propertyNames));
  }

  public PSchema getSchema() {
    return schema;
  }

  protected void addBucketInternal(final PBucket bucket) {
    for (PType cl : schema.getTypes()) {
      if (cl.hasBucket(bucket.getName()))
        throw new PSchemaException("Cannot add the bucket '" + bucket.getName() + "' to the class '" + name
            + "', because the bucket is already associated to the class '" + cl.getName() + "'");
    }

    buckets.add(bucket);
    selectionStrategy.setTotalBuckets(buckets.size());
  }
}
