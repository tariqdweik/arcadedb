/*
 * Copyright 2010-2016 OrientDB LTD (http://orientdb.com)
 * Copyright 2013 Geomatys.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.arcadedb.sql.function.text;

import com.arcadedb.database.PDocument;
import com.arcadedb.database.PIdentifiable;
import com.arcadedb.sql.executor.OCommandContext;
import com.arcadedb.sql.executor.OMultiValue;
import com.arcadedb.sql.method.misc.OAbstractSQLMethod;
import org.json.JSONObject;

import java.util.Map;

/**
 * Converts a document in JSON string.
 *
 * @author Johann Sorel (Geomatys)
 * @author Luca Garulli (l.garulli--(at)--orientdb.com)
 */
public class OSQLMethodToJSON extends OAbstractSQLMethod {

  public static final String NAME = "tojson";

  public OSQLMethodToJSON() {
    super(NAME, 0, 0);
  }

  @Override
  public String getSyntax() {
    return "toJSON()";
  }

  @Override
  public Object execute( Object iThis, PIdentifiable iCurrentRecord, OCommandContext iContext, Object ioResult,
      Object[] iParams) {
    if (iThis == null)
      return null;

    if (iThis instanceof PDocument) {

      return ((PDocument) iThis).toJSON();

    } else if (iThis instanceof Map) {

      return new JSONObject(iThis);

    } else if (OMultiValue.isMultiValue(iThis)) {

      StringBuilder builder = new StringBuilder();
      builder.append("[");
      boolean first = true;
      for (Object o : OMultiValue.getMultiValueIterable(iThis, false)) {
        if (!first) {
          builder.append(",");
        }
        builder.append(execute(o, iCurrentRecord, iContext, ioResult, iParams));
        first = false;
      }

      builder.append("]");
      return builder.toString();
    }
    return null;
  }
}
