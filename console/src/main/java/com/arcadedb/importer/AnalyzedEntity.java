/*
 * Copyright (c) - Arcade Analytics LTD (https://arcadeanalytics.com)
 */

package com.arcadedb.importer;

import com.arcadedb.schema.Type;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

public class AnalyzedEntity {
  public enum ENTITY_TYPE {DOCUMENT, VERTEX, EDGE}

  private final String                        name;
  private final ENTITY_TYPE                   type;
  private final Map<String, AnalyzedProperty> properties;
  private       long                          totalRowLength = 0;
  private       long                          analyzedRows   = 0;
  private       long                          maxValueSampling;

  public AnalyzedEntity(final String name, final ENTITY_TYPE type, final long maxValueSampling) {
    this.name = name;
    this.type = type;
    this.properties = new HashMap<>();
    this.maxValueSampling = maxValueSampling;
  }

  public Collection<AnalyzedProperty> getProperties() {
    return properties.values();
  }

  public AnalyzedProperty getProperty(final String name) {
    return properties.get(name);
  }

  public void getOrCreateProperty(final String name, final String content) {
    AnalyzedProperty property = properties.get(name);
    if (property == null) {
      property = new AnalyzedProperty(name, Type.STRING, maxValueSampling, properties.size());
      properties.put(property.getName(), property);
    }

    property.setLastContent(content);
  }

  public int getAverageRowLength() {
    return (int) (totalRowLength / analyzedRows);
  }

  public void setRowSize(final String[] row) {
    for (int i = 0; i < row.length; ++i) {
      totalRowLength += row[i].length() + 1;
    }
    ++totalRowLength; // ADD LF

    ++analyzedRows;
  }

  public String getName() {
    return name;
  }

  public ENTITY_TYPE getType() {
    return type;
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o)
      return true;
    if (o == null || getClass() != o.getClass())
      return false;
    final AnalyzedEntity that = (AnalyzedEntity) o;
    return Objects.equals(name, that.name);
  }

  @Override
  public int hashCode() {
    return Objects.hash(name);
  }

  @Override
  public String toString() {
    return name;
  }
}
