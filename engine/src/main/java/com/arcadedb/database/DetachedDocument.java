/*
 * Copyright (c) - Arcade Data LTD (https://arcadedata.com)
 */

package com.arcadedb.database;

import org.json.JSONObject;

import java.util.*;

public class DetachedDocument extends ImmutableDocument {
  private Map<String, Object> map;

  protected DetachedDocument(final BaseDocument source) {
    super(null, source.typeName, source.rid, null);
    init(source);
  }

  private void init(Document sourceDocument) {
    this.map = new LinkedHashMap<>();
    this.map.putAll(sourceDocument.toMap());
  }

  @Override
  public Map<String, Object> toMap() {
    return new HashMap<>(map);
  }

  @Override
  public JSONObject toJSON() {
    return new JSONSerializer(database).map2json(map);
  }

  @Override
  public boolean has(String propertyName) {
    return map.containsKey(propertyName);
  }

  public Object get(final String propertyName) {
    return map.get(propertyName);
  }

  @Override
  public void setBuffer(Binary buffer) {
    throw new UnsupportedOperationException("setBuffer");
  }

  @Override
  public String toString() {
    final StringBuilder result = new StringBuilder(256);
    if (rid != null)
      result.append(rid);
    if (typeName != null) {
      result.append('@');
      result.append(typeName);
    }
    result.append('[');
    if (map == null) {
      result.append('?');
    } else {
      int i = 0;
      for (Map.Entry<String, Object> entry : map.entrySet()) {
        if (i > 0)
          result.append(',');

        result.append(entry.getKey());
        result.append('=');
        result.append(entry.getValue());
        i++;
      }
    }
    result.append(']');
    return result.toString();
  }

  @Override
  public Set<String> getPropertyNames() {
    return map.keySet();
  }

  @Override
  public void reload() {
    map = null;
    buffer = null;
    super.reload();
    init(this);
  }
}
