package com.arcadedb.database;

import org.json.JSONObject;

import java.util.Map;
import java.util.Set;

/**
 * Immutable document implementation. To modify the content, call modify() to obtain a modifiable copy.
 */
public class ImmutableDocument extends BaseDocument {

  protected ImmutableDocument(final Database graph, final String typeName, final RID rid, final Binary buffer) {
    super(graph, typeName, rid, buffer);
  }

  @Override
  public Object get(final String name) {
    checkForLazyLoading();
    buffer.position(propertiesStartingPosition);
    final Map<String, Object> map = database.getSerializer().deserializeProperties(database, buffer, name);
    return map.get(name);
  }

  @Override
  public ModifiableDocument modify() {
    checkForLazyLoading();
    // CREATE A SEPARATE OBJECT THAT POINTS TO THE SAME BUFFER TO AVOID CONCURRENCY ON THE BUFFER POSITION
    return new ModifiableDocument(database, typeName, rid, buffer.slice());
  }

  @Override
  public JSONObject toJSON() {
    return new JSONObject(database.getSerializer().deserializeProperties(database, buffer));
  }

  @Override
  public Map<String, Object> toMap() {
    return database.getSerializer().deserializeProperties(database, buffer);
  }

  @Override
  public String toString() {
    final StringBuilder output = new StringBuilder(256);
    if (rid != null)
      output.append(rid);
    output.append('[');
    if (buffer == null)
      output.append('?');
    else {
      final int currPosition = buffer.position();

      buffer.position(propertiesStartingPosition);
      final Map<String, Object> map = this.database.getSerializer().deserializeProperties(database, buffer);

      buffer.position(currPosition);

      int i = 0;
      for (Map.Entry<String, Object> entry : map.entrySet()) {
        if (i > 0)
          output.append(',');

        output.append(entry.getKey());
        output.append('=');
        output.append(entry.getValue());
        i++;
      }
    }
    output.append(']');
    return output.toString();
  }

  @Override
  public Set<String> getPropertyNames() {
    checkForLazyLoading();

    buffer.position(propertiesStartingPosition); // SKIP RECORD BYTE

    return database.getSerializer().getPropertyNames(database, buffer);
  }

  protected boolean checkForLazyLoading() {
    if (buffer == null) {
      if (rid == null)
        throw new RuntimeException("Document cannot be loaded because RID is null");

      buffer = database.getSchema().getBucketById(rid.getBucketId()).getRecord(rid);
      return true;
    }
    return false;
  }
}
