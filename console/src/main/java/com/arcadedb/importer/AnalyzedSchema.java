/*
 * Copyright (c) 2018 - Arcade Analytics LTD (https://arcadeanalytics.com)
 */

package com.arcadedb.importer;

import com.arcadedb.schema.Type;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public class AnalyzedSchema {
  private       String                                     name;
  private       Map<String, Map<String, AnalyzedProperty>> map = new HashMap<>();
  private final long                                       maxValueSampling;

  public AnalyzedSchema(final long maxValueSampling) {
    this.maxValueSampling = maxValueSampling;
  }

  public void setName(final String name) {
    this.name = name;
  }

  public void setProperty(final String entityName, final String name, final String content) {
    Map<String, AnalyzedProperty> entity = map.get(entityName);
    if (entity == null) {
      entity = new HashMap<>();
      map.put(entityName, entity);
    }

    AnalyzedProperty property = entity.get(name);
    if (property == null) {
      property = new AnalyzedProperty(name, Type.STRING, maxValueSampling);
      entity.put(property.getName(), property);
    }

    property.setLastContent(content);
  }

  public void endParsing() {
    for (Map<String, AnalyzedProperty> entity : map.values())
      for (AnalyzedProperty property : entity.values())
        property.endParsing();
  }

  public Set<String> getEntities() {
    return map.keySet();
  }

  public Iterable<? extends Map.Entry<String, AnalyzedProperty>> getProperties(final String entityName) {
    final Map<String, AnalyzedProperty> entity = map.get(entityName);
    return entity != null ? entity.entrySet() : null;
  }
}
