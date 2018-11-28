/*
 * Copyright (c) 2018 - Arcade Analytics LTD (https://arcadeanalytics.com)
 */

package com.arcadedb.importer;

import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.Map;

public class AnalyzedSchema {
  private       String                      name;
  private       Map<String, AnalyzedEntity> entities = new LinkedHashMap<>();
  private final long                        maxValueSampling;

  public AnalyzedSchema(final long maxValueSampling) {
    this.maxValueSampling = maxValueSampling;
  }

  public void setName(final String name) {
    this.name = name;
  }

  public AnalyzedEntity getOrCreateEntity(final String entityName, final AnalyzedEntity.ENTITY_TYPE entityType) {
    AnalyzedEntity entity = entities.get(entityName);
    if (entity == null) {
      entity = new AnalyzedEntity(entityName, entityType, maxValueSampling);
      entities.put(entityName, entity);
    }
    return entity;
  }

  public void endParsing() {
    for (AnalyzedEntity entity : entities.values())
      for (AnalyzedProperty property : entity.getProperties())
        property.endParsing();
  }

  public Collection<AnalyzedEntity> getEntities() {
    return entities.values();
  }

  public AnalyzedEntity getEntity(final String name) {
    return entities.get(name);
  }
}
