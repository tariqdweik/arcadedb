/*
 * Copyright (c) 2018 - Arcade Analytics LTD (https://arcadeanalytics.com)
 */

package com.arcadedb.database;

import org.json.JSONObject;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

public class MutableDocument extends BaseDocument implements RecordInternal {
  private   Map<String, Object> map;
  protected boolean             dirty = false;

  protected MutableDocument(final Database database, final String typeName, final RID rid) {
    super(database, typeName, rid, null);
    this.map = new LinkedHashMap<String, Object>();
  }

  protected MutableDocument(final Database database, final String typeName, final RID rid, final Binary buffer) {
    super(database, typeName, rid, buffer);
    buffer.position(buffer.position() + 1); // SKIP RECORD TYPE
  }

  public void merge(final Document other) {
    for (String p : other.getPropertyNames())
      set(p, other.get(p));
  }

  public boolean isDirty() {
    return dirty;
  }

  @Override
  public void setBuffer(final Binary buffer) {
    super.setBuffer(buffer);
    dirty = false;
    map = null;
  }

  @Override
  public void unsetDirty() {
    map = null;
    dirty = false;
  }

  public void fromMap(final Map<String, Object> map) {
    this.map = new HashMap<>(map);
    dirty = true;
  }

  @Override
  public Map<String, Object> toMap() {
    return new HashMap<>(map);
  }

  public void fromJSON(final JSONObject json) {
    fromMap(json.toMap());
  }

  @Override
  public JSONObject toJSON() {
    checkForLazyLoadingProperties();
    return new JSONObject(map);
  }

  public Object get(final String name) {
    checkForLazyLoadingProperties();
    return map.get(name);
  }

  public void set(final String name, final Object value) {
    checkForLazyLoadingProperties();
    dirty = true;
    map.put(name, value);
  }

  public void set(final Object... properties) {
    if (properties.length % 2 != 0)
      throw new IllegalArgumentException("properties must be an even pair of key/values");

    checkForLazyLoadingProperties();
    dirty = true;

    for (int p = 0; p < properties.length; p += 2)
      map.put((String) properties[p], properties[p + 1]);
  }

  public Object remove(final String name) {
    checkForLazyLoadingProperties();
    dirty = true;
    return map.remove(name);
  }

  public MutableDocument save() {
    if (rid != null)
      database.updateRecord(this);
    else
      database.createRecord(this);
    return this;
  }

  public MutableDocument save(final String bucketName) {
    if (rid != null)
      throw new IllegalStateException("Cannot update a record in a custom bucket");

    database.createRecord(this, bucketName);
    return this;
  }

  @Override
  public void setIdentity(final RID rid) {
    this.rid = rid;
  }

  @Override
  public String toString() {
    final StringBuilder buffer = new StringBuilder(256);
    if (rid != null)
      buffer.append(rid);
    if (typeName != null) {
      buffer.append('@');
      buffer.append(typeName);
    }
    buffer.append('[');
    if (map == null) {
      buffer.append('?');
    } else {
      int i = 0;
      for (Map.Entry<String, Object> entry : map.entrySet()) {
        if (i > 0)
          buffer.append(',');

        buffer.append(entry.getKey());
        buffer.append('=');
        buffer.append(entry.getValue());
        i++;
      }
    }
    buffer.append(']');
    return buffer.toString();
  }

  @Override
  public Set<String> getPropertyNames() {
    checkForLazyLoadingProperties();
    return map.keySet();
  }

  public MutableDocument modify() {
    return this;
  }

  @Override
  public void reload() {
    dirty = false;
    map = null;
    buffer = null;
    super.reload();
  }

  protected void checkForLazyLoadingProperties() {
    if (this.map == null) {
      if (buffer == null)
        reload();

      buffer.position(propertiesStartingPosition);
      this.map = this.database.getSerializer().deserializeProperties(this.database, buffer);
    }
  }
}
