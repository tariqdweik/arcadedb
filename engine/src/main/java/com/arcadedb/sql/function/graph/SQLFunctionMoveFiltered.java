package com.arcadedb.sql.function.graph;

import com.arcadedb.database.Database;
import com.arcadedb.database.Identifiable;
import com.arcadedb.sql.executor.CommandContext;
import com.arcadedb.sql.executor.MultiValue;
import com.arcadedb.sql.executor.SQLEngine;
import com.arcadedb.sql.executor.SQLFunctionFiltered;
import com.arcadedb.utility.Callable;
import com.arcadedb.utility.FileUtils;

/**
 * Created by luigidellaquila on 03/01/17.
 */
public abstract class SQLFunctionMoveFiltered extends SQLFunctionMove implements SQLFunctionFiltered {

  protected static int supernodeThreshold = 1000; // move to some configuration

  public SQLFunctionMoveFiltered() {
    super(NAME, 1, 2);
  }

  public SQLFunctionMoveFiltered(final String iName, final int iMin, final int iMax) {
    super(iName, iMin, iMax);
  }

  @Override
  public Object execute(final Object iThis, final Identifiable iCurrentRecord, final Object iCurrentResult,
      final Object[] iParameters, final Iterable<Identifiable> iPossibleResults, final CommandContext iContext) {
    final String[] labels;
    if (iParameters != null && iParameters.length > 0 && iParameters[0] != null)
      labels = MultiValue.array(iParameters, String.class, new Callable<Object, Object>() {

        @Override
        public Object call(final Object iArgument) {
          return FileUtils.getStringContent(iArgument);
        }
      });
    else
      labels = null;

    return SQLEngine.foreachRecord(new Callable<Object, Identifiable>() {
      @Override
      public Object call(final Identifiable iArgument) {
        return move(iContext.getDatabase(), iArgument, labels, iPossibleResults);
      }
    }, iThis, iContext);

  }

  protected abstract Object move(Database graph, Identifiable iArgument, String[] labels,
      Iterable<Identifiable> iPossibleResults);

}
