/*
 * Copyright (c) - Arcade Data LTD (https://arcadedata.com)
 */

package com.arcadedb.schema;

import com.arcadedb.database.BucketSelectionStrategy;
import com.arcadedb.database.DefaultBucketSelectionStrategy;
import com.arcadedb.database.Document;
import com.arcadedb.engine.Bucket;
import com.arcadedb.exception.SchemaException;
import com.arcadedb.index.Index;
import com.arcadedb.index.TypeIndex;
import com.arcadedb.index.lsm.LSMTreeIndexAbstract;

import java.util.*;

public class DocumentType {
  private final SchemaImpl                   schema;
  private final String                       name;
  private final List<DocumentType>           parentTypes             = new ArrayList<>();
  private final List<DocumentType>           subTypes                = new ArrayList<>();
  private final List<Bucket>                 buckets                 = new ArrayList<>();
  private       BucketSelectionStrategy      bucketSelectionStrategy = new DefaultBucketSelectionStrategy();
  private final Map<String, Property>        properties              = new HashMap<>();
  private       Map<Integer, List<Index>>    indexesByBucket         = new HashMap<>();
  private       Map<List<String>, TypeIndex> indexesByProperties     = new HashMap<>();

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
      // ALREADY PARENT
      return;

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

  public Property createProperty(final String propertyName, final String propertyType) {
    return createProperty(propertyName, Type.getTypeByName(propertyType));
  }

  public Property createProperty(final String propertyName, final Class<?> propertyType) {
    return createProperty(propertyName, Type.getTypeByClass(propertyType));
  }

  public Property createProperty(final String propertyName, final Type propertyType) {
    if (properties.containsKey(propertyName))
      throw new SchemaException("Cannot create the property '" + propertyName + "' in type '" + name + "' because it already exists");

    if (getPolymorphicPropertyNames().contains(propertyName))
      throw new SchemaException("Cannot create the property '" + propertyName + "' in type '" + name + "' because it was already defined in a parent class");

    final Property property = new Property(this, propertyName, propertyType);

    properties.put(propertyName, property);

    schema.saveConfiguration();

    return property;
  }

  public Property getOrCreateProperty(final String propertyName, final String propertyType) {
    return getOrCreateProperty(propertyName, Type.getTypeByName(propertyType));
  }

  public Property getOrCreateProperty(final String propertyName, final Class<?> propertyType) {
    return getOrCreateProperty(propertyName, Type.getTypeByClass(propertyType));
  }

  public Property getOrCreateProperty(final String propertyName, final Type propertyType) {
    Property p = properties.get(propertyName);
    if (p != null) {
      if (p.getType().equals(propertyType))
        return p;

      // DIFFERENT TYPE: DROP THE PROPERTY AND CREATE A NEW ONE
      dropProperty(propertyName);
    }
    return createProperty(propertyName, propertyType);
  }

  public void dropProperty(final String propertyName) {
    properties.remove(propertyName);
    schema.saveConfiguration();
  }

  public TypeIndex createTypeIndex(final SchemaImpl.INDEX_TYPE indexType, final boolean unique, final String... propertyNames) {
    return schema.createTypeIndex(indexType, unique, name, propertyNames);
  }

  public TypeIndex createTypeIndex(final SchemaImpl.INDEX_TYPE indexType, final boolean unique, String[] propertyNames, final int pageSize) {
    return schema.createTypeIndex(indexType, unique, name, propertyNames, pageSize);
  }

  public TypeIndex createTypeIndex(final SchemaImpl.INDEX_TYPE indexType, final boolean unique, final String[] propertyNames, final int pageSize,
      final Index.BuildIndexCallback callback) {
    return schema.createTypeIndex(indexType, unique, name, propertyNames, pageSize, callback);
  }

  public TypeIndex createTypeIndex(final SchemaImpl.INDEX_TYPE indexType, final boolean unique, final String[] propertyNames, final int pageSize,
      LSMTreeIndexAbstract.NULL_STRATEGY nullStrategy, final Index.BuildIndexCallback callback) {
    return schema.createTypeIndex(indexType, unique, name, propertyNames, pageSize, nullStrategy, callback);
  }

  public TypeIndex getOrCreateTypeIndex(final SchemaImpl.INDEX_TYPE indexType, final boolean unique, final String... propertyNames) {
    return schema.getOrCreateTypeIndex(indexType, unique, name, propertyNames);
  }

  public TypeIndex getOrCreateTypeIndex(final SchemaImpl.INDEX_TYPE indexType, final boolean unique, String[] propertyNames, final int pageSize) {
    return schema.getOrCreateTypeIndex(indexType, unique, name, propertyNames, pageSize);
  }

  public TypeIndex getOrCreateTypeIndex(final SchemaImpl.INDEX_TYPE indexType, final boolean unique, final String[] propertyNames, final int pageSize,
      final Index.BuildIndexCallback callback) {
    return schema.getOrCreateTypeIndex(indexType, unique, name, propertyNames, pageSize, callback);
  }

  public TypeIndex getOrCreateTypeIndex(final SchemaImpl.INDEX_TYPE indexType, final boolean unique, final String[] propertyNames, final int pageSize,
      LSMTreeIndexAbstract.NULL_STRATEGY nullStrategy, final Index.BuildIndexCallback callback) {
    return schema.getOrCreateTypeIndex(indexType, unique, name, propertyNames, pageSize, nullStrategy, callback);
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

  public void addBucket(final Bucket bucket) {
    addBucketInternal(bucket);
    schema.saveConfiguration();
  }

  public Bucket getBucketToSave(final boolean async) {
    if (buckets.isEmpty())
      throw new SchemaException("Cannot save on a bucket for type '" + name + "' because there are no buckets associated");
    return buckets.get(bucketSelectionStrategy.getBucketToSave(async));
  }

  public BucketSelectionStrategy getBucketSelectionStrategy() {
    return bucketSelectionStrategy;
  }

  public void setBucketSelectionStrategy(final BucketSelectionStrategy selectionStrategy) {
    this.bucketSelectionStrategy = selectionStrategy;
    this.bucketSelectionStrategy.setTotalBuckets(buckets.size());
  }

  public boolean existsProperty(final String propertyName) {
    return properties.containsKey(propertyName);
  }

  public boolean existsPolymorphicProperty(final String propertyName) {
    return getPolymorphicPropertyNames().contains(propertyName);
  }

  public Property getPolymorphicPropertyIfExists(final String propertyName) {
    return getPolymorphicProperties().get(propertyName);
  }

  public Property getPropertyIfExists(final String propertyName) {
    return properties.get(propertyName);
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

  public TypeIndex[] getAllIndexes() {
    final TypeIndex[] array = new TypeIndex[indexesByProperties.size()];
    int i = 0;
    for (TypeIndex idx : indexesByProperties.values())
      array[i++] = idx;
    return array;
  }

  public List<Index> getSubIndexByBucketId(final int bucketId) {
    return indexesByBucket.get(bucketId);
  }

  public TypeIndex getIndexByProperties(final String... properties) {
    return indexesByProperties.get(Arrays.asList(properties));
  }

  public Schema getSchema() {
    return schema;
  }

  @Override
  public String toString() {
    return name;
  }

  public boolean isTheSameAs(final Object o) {
    if (this == o)
      return true;
    if (o == null || getClass() != o.getClass())
      return false;

    final DocumentType that = (DocumentType) o;
    if (!Objects.equals(name, that.name))
      return false;

    if (parentTypes.size() != that.parentTypes.size())
      return false;

    final Set<String> set = new HashSet<>();
    for (DocumentType t : parentTypes)
      set.add(t.name);

    for (DocumentType t : that.parentTypes)
      set.remove(t.name);

    if (!set.isEmpty())
      return false;

    if (subTypes.size() != that.subTypes.size())
      return false;

    set.clear();

    for (DocumentType t : subTypes)
      set.add(t.name);

    for (DocumentType t : that.subTypes)
      set.remove(t.name);

    if (!set.isEmpty())
      return false;

    if (buckets.size() != that.buckets.size())
      return false;

    set.clear();

    for (Bucket t : buckets)
      set.add(t.getName());

    for (Bucket t : that.buckets)
      set.remove(t.getName());

    if (!set.isEmpty())
      return false;

    if (properties.size() != that.properties.size())
      return false;

    set.clear();

    for (Property p : properties.values())
      set.add(p.getName());

    for (Property p : that.properties.values())
      set.remove(p.getName());

    if (!set.isEmpty())
      return false;

    for (Property p1 : properties.values()) {
      final Property p2 = that.properties.get(p1.getName());
      if (!p1.equals(p2))
        return false;
    }

    if (indexesByBucket.size() != that.indexesByBucket.size())
      return false;

    for (Map.Entry<Integer, List<Index>> entry1 : indexesByBucket.entrySet()) {
      final List<Index> value2 = that.indexesByBucket.get(entry1.getKey());
      if (value2 == null)
        return false;
      if (entry1.getValue().size() != value2.size())
        return false;

      for (int i = 0; i < value2.size(); ++i) {
        final Index m1 = entry1.getValue().get(i);
        final Index m2 = value2.get(i);

        if (m1.getAssociatedBucketId() != m2.getAssociatedBucketId())
          return false;
        if (!m1.getName().equals(m2.getName()))
          return false;
        if (m1.getPropertyNames().length != m2.getPropertyNames().length)
          return false;

        for (int p = 0; p < m1.getPropertyNames().length; ++p) {
          if (!m1.getPropertyNames()[p].equals(m2.getPropertyNames()[p]))
            return false;
        }
      }
    }

    if (indexesByProperties.size() != that.indexesByProperties.size())
      return false;

    for (Map.Entry<List<String>, TypeIndex> entry1 : indexesByProperties.entrySet()) {
      final TypeIndex index2 = that.indexesByProperties.get(entry1.getKey());
      if (index2 == null)
        return false;

      final TypeIndex index1 = entry1.getValue();

      if (!index1.equals(index2))
        return false;
    }

    return true;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o)
      return true;
    if (o == null || getClass() != o.getClass())
      return false;
    DocumentType that = (DocumentType) o;

    return name.equals(that.name);
  }

  @Override
  public int hashCode() {
    return Objects.hash(name);
  }

  protected void addIndexInternal(final Index index, final Bucket bucket, final String[] propertyNames) {
    index.setMetadata(name, propertyNames, bucket.getId());

    List<Index> list = indexesByBucket.get(bucket.getId());
    if (list == null) {
      list = new ArrayList<>();
      indexesByBucket.put(bucket.getId(), list);
    }
    list.add(index);

    final List<String> propertyList = Arrays.asList(propertyNames);

    TypeIndex propIndex = indexesByProperties.get(propertyList);
    if (propIndex == null) {
      // CREATE THE TYPE-INDEX FOR THE 1ST TIME
      propIndex = new TypeIndex(name + Arrays.toString(propertyNames).replace(" ", ""), schema);
      schema.indexMap.put(propIndex.getName(), propIndex);
      indexesByProperties.put(propertyList, propIndex);
    }

    // ADD AS SUB-INDEX
    propIndex.addIndexOnBucket(index);
  }

  public void removeIndexInternal(final String indexName) {
    for (List<Index> indexes : indexesByBucket.values()) {
      for (Iterator<Index> it = indexes.iterator(); it.hasNext(); ) {
        final Index idx = it.next();
        if (indexName.equals(idx.getName()))
          it.remove();
      }
    }

    for (Iterator<TypeIndex> it = indexesByProperties.values().iterator(); it.hasNext(); ) {
      final TypeIndex idx = it.next();
      if (indexName.equals(idx.getName()))
        it.remove();
    }
  }

  protected void addBucketInternal(final Bucket bucket) {
    for (DocumentType cl : schema.getTypes()) {
      if (cl.hasBucket(bucket.getName()))
        throw new SchemaException(
            "Cannot add the bucket '" + bucket.getName() + "' to the type '" + name + "', because the bucket is already associated to the type '" + cl.getName()
                + "'");
    }

    buckets.add(bucket);
    bucketSelectionStrategy.setTotalBuckets(buckets.size());
  }

  protected Map<String, Property> getPolymorphicProperties() {
    final Map<String, Property> allProperties = new HashMap<>();
    allProperties.putAll(properties);

    for (DocumentType p : parentTypes)
      allProperties.putAll(p.getPolymorphicProperties());

    return allProperties;
  }

  private boolean hasBucket(final String bucketName) {
    for (Bucket b : buckets)
      if (b.getName().equals(bucketName))
        return true;
    return false;
  }

  public int getFirstBucketId() {
    return buckets.get(0).getId();
  }
}
