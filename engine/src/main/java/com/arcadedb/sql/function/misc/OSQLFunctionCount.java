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
package com.arcadedb.sql.function.misc;

import com.arcadedb.database.PDatabase;
import com.arcadedb.database.PIdentifiable;
import com.arcadedb.sql.executor.OCommandContext;
import com.arcadedb.sql.function.math.OSQLFunctionMathAbstract;

/**
 * Count the record that contains a field. Use * to indicate the record instead of the field. Uses the context to save the counter
 * number. When different Number class are used, take the class with most precision.
 *
 * @author Luca Garulli (l.garulli--(at)--orientdb.com)
 */
public class OSQLFunctionCount extends OSQLFunctionMathAbstract {
  public static final String NAME = "count";

  private long total = 0;

  public OSQLFunctionCount() {
    super(NAME, 1, 1);
  }

  public Object execute(PDatabase database, Object iThis, PIdentifiable iCurrentRecord, Object iCurrentResult, final Object[] iParams,
      OCommandContext iContext) {
    if (iParams.length == 0 || iParams[0] != null)
      total++;

    return total;
  }

  public boolean aggregateResults() {
    return true;
  }

  public String getSyntax() {
    return "count(<field>|*)";
  }

  @Override
  public Object getResult() {
    return total;
  }

  @Override
  public void setResult(final Object iResult) {
    total = ((Number) iResult).longValue();
  }
}
