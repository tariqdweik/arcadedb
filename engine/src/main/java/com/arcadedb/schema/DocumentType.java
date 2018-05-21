/*
 * Copyright (c) 2018 - Arcade Analytics LTD (https://arcadeanalytics.com)
 */

package com.arcadedb.schema;

import com.arcadedb.database.BucketSelectionStrategy;
import com.arcadedb.database.Document;
import com.arcadedb.database.RoundRobinBucketSelectionStrategy;
import com.arcadedb.engine.Bucket;
import com.arcadedb.exception.SchemaException;
import com.arcadedb.index.Index;

import java.util.*;

public class DocumentType {
  private final SchemaImpl                             schema;
  private final String                                 name;
  private final List<DocumentType>                     parentTypes            = new ArrayList<>();
  private final List<DocumentType>                     subTypes               = new ArrayList<>();
  private final List<Bucket>                           buckets                = new ArrayList<>();
  private       BucketSelectionStrategy                syncSelectionStrategy  = new RoundRobinBucketSelectionStrategy();
  private       BucketSelectionStrategy                asyncSelectionStrategy = new RoundRobinBucketSelectionStrategy();
  private final Map<String, Property>                  properties             = new HashMap<>();
  private       Map<Integer, List<IndexMetadata>>      indexesByBucket        = new HashMap<>();
  private       Map<List<String>, List<IndexMetadata>> indexesByProperties    = new HashMap<>();

  public class IndexMetadata {
    public String[] propertyNames;
    public int      bucketId;
    public Index    index;

    public IndexMetadata(final Index index, final int bucketId, final String[] propertyNames) {
      this.index = index;
      this.bucketId = bucketId;
      this.propertyNames = propertyNames;
    }
  }

  public DocumentType(final SchemaImpl schema, final String name) {
    this.schema = schema;
    this.name = name;
  }

  public String getName() {
    return name;
  }

  public byte getType() {
    return Document.RECORD_TYPE;
  }

  public void addParent(final String parentName) {
    addParent(schema.getType(parentName));
  }

  public void addParent(final DocumentType parent) {
    if (parentTypes.indexOf(parent) > -1)
      throw new IllegalArgumentException("Type '" + parent + "' is already a parent type for '" + name + "'");

    final Set<String> allProperties = getPolymorphicPropertyNames();
    for (String p : parent.getPropertyNames())
      if (allProperties.contains(p))
        throw new IllegalArgumentException("Property '" + p + "' is already defined in type '" + name + "' or any parent types");

    parentTypes.add(parent);
    parent.subTypes.add(this);
    schema.saveConfiguration();
  }

  public boolean instanceOf(final String type) {
    if (name.equals(type))
      return true;

    for (DocumentType t : parentTypes) {
      if (t.instanceOf(type))
        return true;
    }

    return false;
  }

  public List<DocumentType> getParentTypes() {
    return parentTypes;
  }

  public List<DocumentType> getSubTypes() {
    return subTypes;
  }

  public Set<String> getPropertyNames() {
    return properties.keySet();
  }

  public Set<String> getPolymorphicPropertyNames() {
    final Set<String> allProperties = new HashSet<>();
    for (DocumentType p : parentTypes)
      allProperties.addAll(p.getPropertyNames());
    return allProperties;
  }

  public Property createProperty(final String propertyName, final Class<?> propertyType) {
    if (properties.containsKey(propertyName))
      throw new SchemaException(
          "Cannot create the property '" + propertyName + "' in type '" + name + "' because it already exists");

    if (getPolymorphicPropertyNames().contains(propertyName))
      throw new SchemaException("Cannot create the property '" + propertyName + "' in type '" + name
          + "' because it was already defined in a parent class");

    final Property property = new Property(this, propertyName, propertyType);

    properties.put(propertyName, property);

    return property;
  }

  public List<Bucket> getBuckets(final boolean polymorphic) {
    if (!polymorphic)
      return buckets;

    final List<Bucket> allBuckets = new ArrayList<>();
    allBuckets.addAll(buckets);

    for (DocumentType p : subTypes)
      allBuckets.addAll(p.getBuckets(true));

    return allBuckets;
  }

  private boolean hasBucket(final String bucketName) {
    for (Bucket b : buckets)
      if (b.getName().equals(bucketName))
        return true;
    return false;
  }

  public void addBucket(final Bucket bucket) {
    addBucketInternal(bucket);
    schema.saveConfiguration();
  }

  public Bucket getBucketToSave(final boolean async) {
    if (buckets.isEmpty())
      throw new SchemaException("Cannot save on a bucket for type '" + name + "' because there are no buckets associated");
    return buckets.get(async ? asyncSelectionStrategy.getBucketToSave() : syncSelectionStrategy.getBucketToSave());
  }

  public BucketSelectionStrategy getSyncSelectionStrategy() {
    return syncSelectionStrategy;
  }

  public void setSyncSelectionStrategy(final BucketSelectionStrategy selectionStrategy) {
    this.syncSelectionStrategy = selectionStrategy;
    this.syncSelectionStrategy.setTotalBuckets(buckets.size());
  }

  public BucketSelectionStrategy getAsyncSelectionStrategy() {
    return asyncSelectionStrategy;
  }

  public void setAsyncSelectionStrategy(final BucketSelectionStrategy selectionStrategy) {
    this.asyncSelectionStrategy = selectionStrategy;
    this.asyncSelectionStrategy.setTotalBuckets(buckets.size());
  }

  public boolean existsProperty(final String propertyName) {
    return properties.containsKey(propertyName);
  }

  public boolean existsPolymorphicProperty(final String propertyName) {
    return getPolymorphicPropertyNames().contains(propertyName);
  }

  public Property getProperty(final String propertyName) {
    final Property prop = properties.get(propertyName);
    if (prop == null)
      throw new SchemaException("Cannot find property '" + propertyName + "' in type '" + name + "'");
    return prop;
  }

  public Property getPolymorphicProperty(final String propertyName) {
    final Property prop = getPolymorphicProperties().get(propertyName);
    if (prop == null)
      throw new SchemaException("Cannot find property '" + propertyName + "' in type '" + name + "'");
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

  public Schema getSchema() {
    return schema;
  }

  @Override
  public String toString() {
    return name;
  }

  protected void addIndexInternal(final Index index, final Bucket bucket, final String[] propertyNames) {
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

  protected void addBucketInternal(final Bucket bucket) {
    for (DocumentType cl : schema.getTypes()) {
      if (cl.hasBucket(bucket.getName()))
        throw new SchemaException("Cannot add the bucket '" + bucket.getName() + "' to the type '" + name
            + "', because the bucket is already associated to the type '" + cl.getName() + "'");
    }

    buckets.add(bucket);
    syncSelectionStrategy.setTotalBuckets(buckets.size());
    asyncSelectionStrategy.setTotalBuckets(buckets.size());
  }

  protected Map<String, Property> getPolymorphicProperties() {
    final Map<String, Property> allProperties = new HashMap<>();
    allProperties.putAll(properties);

    for (DocumentType p : parentTypes)
      allProperties.putAll(p.getPolymorphicProperties());

    return allProperties;
  }
}
