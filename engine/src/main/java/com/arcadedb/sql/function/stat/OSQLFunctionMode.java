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
package com.arcadedb.sql.function.stat;

import com.arcadedb.database.PDatabase;
import com.arcadedb.database.PIdentifiable;
import com.arcadedb.sql.executor.OCommandContext;
import com.arcadedb.sql.executor.OMultiValue;
import com.arcadedb.sql.function.OSQLFunctionAbstract;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Compute the mode (or multimodal) value for a field. The scores in the field's distribution that occurs more frequently. Nulls are
 * ignored in the calculation.
 *
 * @author Fabrizio Fortino
 */
public class OSQLFunctionMode extends OSQLFunctionAbstract {

  public static final String NAME = "mode";

  private Map<Object, Integer> seen     = new HashMap<Object, Integer>();
  private int                  max      = 0;
  private List<Object>         maxElems = new ArrayList<Object>();

  public OSQLFunctionMode() {
    super(NAME, 1, 1);
  }

  @Override
  public Object execute(final PDatabase database, Object iThis, PIdentifiable iCurrentRecord, Object iCurrentResult,
      Object[] iParams, OCommandContext iContext) {

    if (OMultiValue.isMultiValue(iParams[0])) {
      for (Object o : OMultiValue.getMultiValueIterable(iParams[0])) {
        max = evaluate(o, 1, seen, maxElems, max);
      }
    } else {
      max = evaluate(iParams[0], 1, seen, maxElems, max);
    }
    return getResult();
  }

  @Override
  public Object getResult() {
    return maxElems.isEmpty() ? null : maxElems;
  }

  @Override
  public String getSyntax() {
    return NAME + "(<field>)";
  }

  @Override
  public boolean aggregateResults() {
    return true;
  }

  private int evaluate(Object value, int times, Map<Object, Integer> iSeen, List<Object> iMaxElems, int iMax) {
    if (value != null) {
      if (iSeen.containsKey(value)) {
        iSeen.put(value, iSeen.get(value) + times);
      } else {
        iSeen.put(value, times);
      }
      if (iSeen.get(value) > iMax) {
        iMax = iSeen.get(value);
        iMaxElems.clear();
        iMaxElems.add(value);
      } else if (iSeen.get(value) == iMax) {
        iMaxElems.add(value);
      }
    }
    return iMax;
  }

}
