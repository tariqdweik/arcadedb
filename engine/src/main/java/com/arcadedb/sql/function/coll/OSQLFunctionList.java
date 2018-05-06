/*
 *
 *  *  Copyright 2010-2016 OrientDB LTD (http://orientdb.com)
 *  *
 *  *  Licensed under the Apache License, Version 2.0 (the "License");
 *  *  you may not use this file except in compliance with the License.
 *  *  You may obtain a copy of the License at
 *  *
 *  *       http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  *  Unless required by applicable law or agreed to in writing, software
 *  *  distributed under the License is distributed on an "AS IS" BASIS,
 *  *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  *  See the License for the specific language governing permissions and
 *  *  limitations under the License.
 *  *
 *  * For more information: http://orientdb.com
 *
 */
package com.arcadedb.sql.function.coll;

import com.arcadedb.database.PDatabase;
import com.arcadedb.database.PIdentifiable;
import com.arcadedb.sql.executor.OCommandContext;
import com.arcadedb.sql.executor.OMultiValue;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * This operator add an item in a list. The list accepts duplicates.
 *
 * @author Luca Garulli (l.garulli--(at)--orientdb.com)
 */
public class OSQLFunctionList extends OSQLFunctionMultiValueAbstract<List<Object>> {
  public static final String NAME = "list";

  public OSQLFunctionList() {
    super(NAME, 1, -1);
  }

  public Object execute(PDatabase database, Object iThis, final PIdentifiable iCurrentRecord, Object iCurrentResult, final Object[] iParams,
      OCommandContext iContext) {
    if (iParams.length > 1)
      // IN LINE MODE
      context = new ArrayList<>();

    for (Object value : iParams) {
      if (value != null) {
        if (iParams.length == 1 && context == null)
          // AGGREGATION MODE (STATEFULL)
          context = new ArrayList<Object>();

        if (value instanceof Map)
          context.add(value);
        else
          OMultiValue.add(context, value);
      }
    }
    return context;
  }

  public String getSyntax() {
    return "list(<value>*)";
  }

  public boolean aggregateResults(final Object[] configuredParameters) {
    return false;
  }
}
