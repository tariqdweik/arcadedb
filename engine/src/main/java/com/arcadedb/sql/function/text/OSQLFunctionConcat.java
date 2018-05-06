package com.arcadedb.sql.function.text;

import com.arcadedb.database.PDatabase;
import com.arcadedb.database.PIdentifiable;
import com.arcadedb.sql.executor.OCommandContext;
import com.arcadedb.sql.function.OSQLFunctionConfigurableAbstract;

public class OSQLFunctionConcat extends OSQLFunctionConfigurableAbstract {
  public static final String        NAME = "concat";
  private             StringBuilder sb;

  public OSQLFunctionConcat() {
    super(NAME, 1, 2);
  }

  @Override
  public Object execute(final PDatabase database, final Object iThis, PIdentifiable iCurrentRecord, Object iCurrentResult,
      Object[] iParams, OCommandContext iContext) {
    if (sb == null) {
      sb = new StringBuilder();
    } else {
      if (iParams.length > 1)
        sb.append(iParams[1]);
    }
    sb.append(iParams[0]);
    return null;
  }

  @Override
  public Object getResult() {
    return sb != null ? sb.toString() : null;
  }

  @Override
  public String getSyntax() {
    return "concat(<field>, [<delim>])";
  }

  @Override
  public boolean aggregateResults() {
    return true;
  }

}
