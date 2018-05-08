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
package com.arcadedb.sql.function.text;

import com.arcadedb.database.Identifiable;
import com.arcadedb.sql.executor.CommandContext;
import com.arcadedb.sql.function.SQLFunctionAbstract;

/**
 * Formats content.
 *
 * @author Luca Garulli (l.garulli--(at)--orientdb.com)
 */
public class SQLFunctionFormat extends SQLFunctionAbstract {
  public static final String NAME = "format";

  public SQLFunctionFormat() {
    super(NAME, 1, -1);
  }

  public Object execute( final Object iThis, Identifiable iCurrentRecord, Object iCurrentResult,
      final Object[] params, CommandContext iContext) {
    final Object[] args = new Object[params.length - 1];

    for (int i = 0; i < args.length; ++i)
      args[i] = params[i + 1];

    return String.format((String) params[0], args);
  }

  public String getSyntax() {
    return "format(<format>, <arg1> [,<argN>]*)";
  }
}
