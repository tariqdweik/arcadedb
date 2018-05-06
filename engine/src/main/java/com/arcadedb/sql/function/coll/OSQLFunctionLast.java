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
import com.arcadedb.sql.function.OSQLFunctionConfigurableAbstract;

/**
 * Extract the last item of multi values (arrays, collections and maps) or return the same value for non multi-value types.
 *
 * @author Luca Garulli (l.garulli--(at)--orientdb.com)
 */
public class OSQLFunctionLast extends OSQLFunctionConfigurableAbstract {
  public static final String NAME = "last";

  public OSQLFunctionLast() {
    super(NAME, 1, 1);
  }

  public Object execute(PDatabase database, Object iThis, final PIdentifiable iCurrentRecord, Object iCurrentResult, final Object[] iParams,
      final OCommandContext iContext) {
    final Object value = iParams[0];

    if (OMultiValue.isMultiValue(value))
      return OMultiValue.getLastValue(value);

    return null;
  }

  public String getSyntax() {
    return "last(<field>)";
  }
}
