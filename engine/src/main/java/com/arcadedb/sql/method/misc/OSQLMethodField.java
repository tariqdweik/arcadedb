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

/**
 * @author Johann Sorel (Geomatys)
 * @author Luca Garulli (l.garulli--(at)--orientdb.com)
 */
public class OSQLMethodField extends OAbstractSQLMethod {

  public static final String NAME = "field";

  public OSQLMethodField() {
    super(NAME, 1, 1);
  }

  @Override
  public Object execute(PDatabase database, Object iThis, final PIdentifiable iCurrentRecord, final OCommandContext iContext, Object ioResult,
      final Object[] iParams) {
    if (iParams[0] == null)
      return null;

    final String paramAsString = iParams[0].toString();

    if (ioResult instanceof PIdentifiable) {
      final PDocument doc = (PDocument) ((PIdentifiable) ioResult).getRecord();
      return doc.get(paramAsString);
    }

    return null;
  }

  @Override
  public boolean evaluateParameters() {
    return false;
  }
}
