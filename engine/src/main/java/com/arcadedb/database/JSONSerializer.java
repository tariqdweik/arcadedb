/*
 * Copyright (c) 2019 - Arcade Data LTD (https://arcadedata.com)
 */

package com.arcadedb.database;

import com.arcadedb.graph.MutableEmbeddedDocument;
import com.arcadedb.graph.MutableVertex;
import com.arcadedb.log.LogManager;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.VertexType;
import org.json.JSONArray;
import org.json.JSONObject;

import java.util.*;
import java.util.logging.Level;

public class JSONSerializer {
  private final Database database;

  public JSONSerializer(final Database database) {
    this.database = database;
  }

  public JSONObject map2json(final Map<String, Object> map) {
    final JSONObject json = new JSONObject();
    for (String k : map.keySet()) {
      final Object value = convertToJSONType(map.get(k));

      if (value instanceof Number && !Float.isFinite(((Number) value).floatValue())) {
        LogManager.instance().log(this, Level.SEVERE, "Found non finite number in map with key '%s', ignore this entry in the conversion", null, k);
        continue;
      }

      json.put(k, value);
    }

    return json;
  }

  public Map<String, Object> json2map(final JSONObject json) {
    final Map<String, Object> map = new HashMap<>();
    for (String k : json.keySet()) {
      final Object value = convertFromJSONType(json.get(k));
      map.put(k, value);
    }

    return map;
  }

  private Object convertToJSONType(Object value) {
    if (value instanceof Document) {
      final JSONObject json = ((Document) value).toJSON();
      json.put("@type", ((Document) value).getType());
      value = json;
    } else if (value instanceof Collection) {
      final Collection c = (Collection) value;
      final JSONArray array = new JSONArray();
      for (Iterator it = c.iterator(); it.hasNext(); )
        array.put(convertToJSONType(it.next()));
      value = array;
    } else if (value instanceof Date)
      value = ((Date) value).getTime();
    else if (value instanceof Map)
      value = new JSONObject((Map) value);

    return value;
  }

  private Object convertFromJSONType(Object value) {
    if (value instanceof JSONObject) {
      final JSONObject json = (JSONObject) value;
      final String embeddedTypeName = json.getString("@type");

      final DocumentType type = database.getSchema().getType(embeddedTypeName);

      if (type instanceof VertexType) {
        final MutableVertex v = database.newVertex(embeddedTypeName);
        v.fromJSON((JSONObject) value);
        value = v;
      } else if (type instanceof DocumentType) {
        final MutableEmbeddedDocument embeddedDocument = database.newEmbeddedDocument(embeddedTypeName);
        embeddedDocument.fromJSON((JSONObject) value);
        value = embeddedDocument;
      }
    } else if (value instanceof JSONArray) {
      final JSONArray array = (JSONArray) value;
      final List<Object> list = new ArrayList<>();
      for (int i = 0; i < array.length(); ++i)
        list.add(convertFromJSONType(array.get(i)));
      value = list;
    }

    return value;
  }
}
