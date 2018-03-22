package com.arcadedb.database;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

public class PModifiableDocument extends PBaseRecord implements PModifiableRecord, PRecordInternal {
  private final Map<String, Object> map;

  protected PModifiableDocument(final PDatabase database, final String typeName, final PRID rid) {
    super(database, typeName, rid);
    this.map = new LinkedHashMap<String, Object>();
  }

  protected PModifiableDocument(final PDatabase database, final String typeName, final PRID rid, final PBinary buffer) {
    super(database, typeName, rid);
    this.map = this.database.getSerializer().deserializeFields(this.database, buffer);
  }

  @Override
  public void set(final String name, final Object value) {
    map.put(name, value);
  }

  @Override
  public Object get(final String name) {
    return map.get(name);
  }

  @Override
  public void save() {
    ((PDatabaseInternal) database).saveRecord(this);
  }

  @Override
  public void save(final String bucketName) {
    ((PDatabaseInternal) database).saveRecord(this, bucketName);
  }

  @Override
  public void setIdentity(final PRID rid) {
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
    int i = 0;
    for (Map.Entry<String, Object> entry : map.entrySet()) {
      if (i > 0)
        buffer.append(',');

      buffer.append(entry.getKey());
      buffer.append('=');
      buffer.append(entry.getValue());
      i++;
    }
    buffer.append(']');
    return buffer.toString();
  }

  @Override
  public Set<String> getPropertyNames() {
    return map.keySet();
  }
}
