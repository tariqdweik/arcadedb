/*
 * Copyright (c) - Arcade Data LTD (https://arcadedata.com)
 */

package com.arcadedb.serializer;

import com.arcadedb.database.Document;
import com.arcadedb.sql.executor.Result;
import org.json.JSONObject;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

public class JsonSerializer {

  public JSONObject serializeRecord(final Document record) {
    final JSONObject object = new JSONObject();

    for (String p : record.getPropertyNames()) {
      Object value = record.get(p);

      if (value instanceof Document)
        value = serializeRecord((Document) value);
      else if (value instanceof Collection) {
        final List<Object> list = new ArrayList<>();
        for (Object o : (Collection) value) {
          if (o instanceof Document)
            o = serializeRecord((Document) o);
          list.add(o);
        }
        value = list;
      }
      object.put(p, value);
    }

    return object;
  }

  public JSONObject serializeResult(final Result record) {
    final JSONObject object = new JSONObject();

    for (String p : record.getPropertyNames()) {
      Object value = record.getProperty(p);

      if (value instanceof Document)
        value = serializeRecord((Document) value);
      else if (value instanceof Result)
        value = serializeResult((Result) value);
      else if (value instanceof Collection) {
        final List<Object> list = new ArrayList<>();
        for (Object o : (Collection) value) {
          if (o instanceof Document)
            o = serializeRecord((Document) o);
          else if (o instanceof Result)
            o = serializeResult((Result) o);
          list.add(o);
        }
        value = list;
      }
      object.put(p, value);
    }

    return object;
  }
}
