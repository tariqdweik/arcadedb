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
package com.arcadedb.sql.method.misc;

import com.arcadedb.database.PDatabase;
import com.arcadedb.database.PDocument;
import com.arcadedb.database.PIdentifiable;
import com.arcadedb.sql.executor.OCommandContext;

import java.util.*;

/**
 * @author Johann Sorel (Geomatys)
 * @author Luca Garulli (l.garulli--(at)--orientdb.com)
 */
public class OSQLMethodKeys extends OAbstractSQLMethod {

  public static final String NAME = "keys";

  public OSQLMethodKeys() {
    super(NAME);
  }

  @Override
  public Object execute(final PDatabase database, final Object iThis, PIdentifiable iCurrentRecord, OCommandContext iContext,
      Object ioResult, Object[] iParams) {
    if (ioResult instanceof Map) {
      return ((Map<?, ?>) ioResult).keySet();
    }
    if (ioResult instanceof PDocument) {
      return Arrays.asList(((PDocument) ioResult).getPropertyNames());
    }
    if (ioResult instanceof Collection) {
      List result = new ArrayList();
      for (Object o : (Collection) ioResult) {
        result.addAll((Collection) execute(database, iThis, iCurrentRecord, iContext, o, iParams));
      }
      return result;
    }
    return null;
  }
}
