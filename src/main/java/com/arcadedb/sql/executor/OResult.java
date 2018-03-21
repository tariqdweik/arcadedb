package com.arcadedb.sql.executor;

import com.arcadedb.database.PEdge;
import com.arcadedb.database.PRID;
import com.arcadedb.database.PRecord;
import com.arcadedb.database.PVertex;

import java.text.SimpleDateFormat;
import java.util.*;

/**
 * Created by luigidellaquila on 21/07/16.
 */
public interface OResult {

  /**
   * returns a property from the result
   *
   * @param name the property name
   * @param <T>
   *
   * @return the property value. If the property value is a persistent record, it only returns the RID. See also  {@link
   * #getElementProperty(String)}
   */
  <T> T getProperty(String name);

  /**
   * returns an OElement property from the result
   *
   * @param name the property name
   *
   * @return the property value. Null if the property is not defined or if it's not an OElement
   */
  PRecord getElementProperty(String name);

  Set<String> getPropertyNames();

  Optional<PRID> getIdentity();

  boolean isElement();

  Optional<PRecord> getElement();

  PRecord toElement();

  Optional<PRecord> getRecord();

  default boolean isRecord() {
    return !isProjection();
  }

  boolean isProjection();

  /**
   * return metadata related to current result given a key
   *
   * @param key the metadata key
   *
   * @return metadata related to current result given a key
   */
  Object getMetadata(String key);

  /**
   * return all the metadata keys available
   *
   * @return all the metadata keys available
   */
  Set<String> getMetadataKeys();

  default String toJSON() {
    //TODO
    return "{}";
  }

  default String toJson(Object val) {
    String jsonVal = null;
    if (val == null) {
      jsonVal = "null";
    } else if (val instanceof String) {
      jsonVal = "\"" + encode(val.toString()) + "\"";
    } else if (val instanceof Number || val instanceof Boolean) {
      jsonVal = val.toString();
    } else if (val instanceof OResult) {
      jsonVal = ((OResult) val).toJSON();
    } else if (val instanceof PRecord) {
      PRID id = ((PRecord) val).getIdentity();

      jsonVal = "\"" + id + "\"";
    } else if (val instanceof PRID) {
      jsonVal = "\"" + val + "\"";
    } else if (val instanceof Iterable) {
      StringBuilder builder = new StringBuilder();
      builder.append("[");
      boolean first = true;
      Iterator iterator = ((Iterable) val).iterator();
      while (iterator.hasNext()) {
        if (!first) {
          builder.append(", ");
        }
        builder.append(toJson(iterator.next()));
        first = false;
      }
      builder.append("]");
      jsonVal = builder.toString();
    } else if (val instanceof Iterator) {
      StringBuilder builder = new StringBuilder();
      builder.append("[");
      boolean first = true;
      Iterator iterator = (Iterator) val;
      while (iterator.hasNext()) {
        if (!first) {
          builder.append(", ");
        }
        builder.append(toJson(iterator.next()));
        first = false;
      }
      builder.append("]");
      jsonVal = builder.toString();
    } else if (val instanceof Map) {
      StringBuilder builder = new StringBuilder();
      builder.append("{");
      boolean first = true;
      Map<Object, Object> map = (Map) val;
      for (Map.Entry entry : map.entrySet()) {
        if (!first) {
          builder.append(", ");
        }
        builder.append(toJson(entry.getKey()));
        builder.append(": ");
        builder.append(toJson(entry.getValue()));
        first = false;
      }
      builder.append("}");
      jsonVal = builder.toString();
    } else if (val instanceof byte[]) {
      jsonVal = "\"" + Base64.getEncoder().encodeToString((byte[]) val) + "\"";
    } else if (val instanceof Date) {
      new SimpleDateFormat().format(val);//TODO
//      jsonVal = "\"" + ODateHelper.getDateTimeFormatInstance().format(val) + "\"";
    } else {

      throw new UnsupportedOperationException("Cannot convert " + val + " - " + val.getClass() + " to JSON");
    }
    return jsonVal;
  }

  default String encode(String s) {
    String result = s.replaceAll("\"", "\\\\\"");
    result = result.replaceAll("\n", "\\\\n");
    result = result.replaceAll("\t", "\\\\t");
    return result;
  }

  default boolean isEdge() {
    return getElement().map(x -> x instanceof PEdge).orElse(false);
  }

  default boolean isVertex() {
    return getElement().map(x -> x instanceof PVertex).orElse(false);
  }

  boolean hasProperty(String varName);
}
