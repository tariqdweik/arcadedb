package com.arcadedb.serializer;

import com.arcadedb.database.PDocument;
import com.arcadedb.sql.executor.OResult;
import org.json.JSONObject;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

public class PJsonSerializer {

  public JSONObject serializeRecord(final PDocument record) {
    final JSONObject object = new JSONObject();

    for (String p : record.getPropertyNames()) {
      Object value = record.get(p);

      if (value instanceof PDocument)
        value = serializeRecord((PDocument) value);
      else if (value instanceof Collection) {
        final List<Object> list = new ArrayList<>();
        for (Object o : (Collection) value) {
          if (o instanceof PDocument)
            o = serializeRecord((PDocument) o);
          list.add(o);
        }
        value = list;
      }
      object.put(p, value);
    }

    return object;
  }

  public JSONObject serializeResult(final OResult record) {
    final JSONObject object = new JSONObject();

    for (String p : record.getPropertyNames()) {
      Object value = record.getProperty(p);

      if (value instanceof PDocument)
        value = serializeRecord((PDocument) value);
      else if (value instanceof OResult)
        value = serializeResult((OResult) value);
      else if (value instanceof Collection) {
        final List<Object> list = new ArrayList<>();
        for (Object o : (Collection) value) {
          if (o instanceof PDocument)
            o = serializeRecord((PDocument) o);
          else if (o instanceof OResult)
            o = serializeResult((OResult) o);
          list.add(o);
        }
        value = list;
      }
      object.put(p, value);
    }

    return object;
  }
}
