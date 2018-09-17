/*
 * Copyright (c) 2018 - Arcade Analytics LTD (https://arcadeanalytics.com)
 */

package com.arcadedb.analyzer;

import com.arcadedb.schema.Type;

import java.util.HashMap;
import java.util.Map;

public class AnalyzedSchema {
  private       HashMap<String, AnalyzedProperty> map = new HashMap<>();
  private final long                              maxValueSampling;

  public AnalyzedSchema(final long maxValueSampling) {
    this.maxValueSampling = maxValueSampling;
  }

  public void set(final String name, final String content) {
    AnalyzedProperty property = map.get(name);
    if (property == null) {
      property = new AnalyzedProperty(name, Type.STRING, maxValueSampling);
      map.put(property.getName(), property);
    }

    property.setLastContent(content);
  }

  public void endParsing() {
    for (AnalyzedProperty p : map.values())
      p.endParsing();
  }

  public Iterable<? extends Map.Entry<String, AnalyzedProperty>> properties() {
    return map.entrySet();
  }
}
