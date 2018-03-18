package com.arcadedb.database;

import java.util.Map;
import java.util.Set;

/**
 * Immutable document implementation. To modify the content, call modify() to obtain a modifiable copy.
 */
public class PImmutableDocument extends PBaseDocument {
  private final PBinary buffer;

  protected PImmutableDocument(final PDatabase graph, final String typeName, final PRID rid, final PBinary buffer) {
    super(graph, typeName, rid);
    this.buffer = buffer;
  }

  @Override
  public Object get(final String name) {
    final Map<String, Object> map = database.getSerializer().deserializeFields(database, buffer, name);
    return map.get(name);
  }

  public PModifiableDocument modify() {
    return new PModifiableDocument(database, typeName, rid, buffer);
  }

  @Override
  public String toString() {
    final StringBuilder buffer = new StringBuilder(256);
    if (rid != null)
      buffer.append(rid);
    buffer.append('[');
    int i = 0;
    for (String name : getPropertyNames()) {
      if (i > 0)
        buffer.append(',');

      buffer.append(name);
      buffer.append('=');
      buffer.append(get(name));
      i++;
    }
    buffer.append(']');
    return buffer.toString();
  }

  @Override
  public Set<String> getPropertyNames() {
    return database.getSerializer().getPropertyNames(database, buffer);
  }
}
