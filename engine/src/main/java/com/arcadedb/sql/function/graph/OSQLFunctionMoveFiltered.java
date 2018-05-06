package com.arcadedb.sql.function.graph;

import com.arcadedb.database.PDatabase;
import com.arcadedb.database.PIdentifiable;
import com.arcadedb.sql.executor.OCommandContext;
import com.arcadedb.sql.executor.OMultiValue;
import com.arcadedb.sql.executor.OSQLEngine;
import com.arcadedb.sql.executor.OSQLFunctionFiltered;
import com.arcadedb.utility.PCallable;
import com.arcadedb.utility.PFileUtils;

/**
 * Created by luigidellaquila on 03/01/17.
 */
public abstract class OSQLFunctionMoveFiltered extends OSQLFunctionMove implements OSQLFunctionFiltered {

  protected static int supernodeThreshold = 1000; // move to some configuration

  public OSQLFunctionMoveFiltered() {
    super(NAME, 1, 2);
  }

  public OSQLFunctionMoveFiltered(final String iName, final int iMin, final int iMax) {
    super(iName, iMin, iMax);
  }

  @Override
  public Object execute(final Object iThis, final PIdentifiable iCurrentRecord, final Object iCurrentResult,
      final Object[] iParameters, final Iterable<PIdentifiable> iPossibleResults, final OCommandContext iContext) {
    final String[] labels;
    if (iParameters != null && iParameters.length > 0 && iParameters[0] != null)
      labels = OMultiValue.array(iParameters, String.class, new PCallable<Object, Object>() {

        @Override
        public Object call(final Object iArgument) {
          return PFileUtils.getStringContent(iArgument);
        }
      });
    else
      labels = null;

    return OSQLEngine.foreachRecord(new PCallable<Object, PIdentifiable>() {
      @Override
      public Object call(final PIdentifiable iArgument) {
        return move(iContext.getDatabase(), iArgument, labels, iPossibleResults);
      }
    }, iThis, iContext);

  }

  protected abstract Object move(PDatabase graph, PIdentifiable iArgument, String[] labels,
      Iterable<PIdentifiable> iPossibleResults);

}
