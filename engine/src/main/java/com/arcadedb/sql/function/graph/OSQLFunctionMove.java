package com.arcadedb.sql.function.graph;

import com.arcadedb.database.PDatabase;
import com.arcadedb.database.PIdentifiable;
import com.arcadedb.graph.PVertex;
import com.arcadedb.sql.executor.OCommandContext;
import com.arcadedb.sql.executor.OMultiValue;
import com.arcadedb.sql.executor.OSQLEngine;
import com.arcadedb.sql.function.OSQLFunctionConfigurableAbstract;
import com.arcadedb.utility.PCallable;
import com.arcadedb.utility.PFileUtils;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by luigidellaquila on 03/01/17.
 */
public abstract class OSQLFunctionMove extends OSQLFunctionConfigurableAbstract {
  public static final String NAME = "move";

  public OSQLFunctionMove() {
    super(NAME, 1, 2);
  }

  public OSQLFunctionMove(final String iName, final int iMin, final int iMax) {
    super(iName, iMin, iMax);
  }

  protected abstract Object move(final PDatabase db, final PIdentifiable iRecord, final String[] iLabels);

  public String getSyntax() {
    return "Syntax error: " + name + "([<labels>])";
  }

  public Object execute(final PDatabase database, final Object iThis, final PIdentifiable iCurrentRecord, final Object iCurrentResult,
      final Object[] iParameters, final OCommandContext iContext) {

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
        return move(database, iArgument, labels);
      }
    }, iThis, iContext);

  }

  protected Object v2v(final PDatabase graph, final PIdentifiable iRecord, final PVertex.DIRECTION iDirection, final String[] iLabels) {
    OElement rec = iRecord.getRecord();
    if (rec.isVertex()) {
      return rec.asVertex().get().getVertices(iDirection, iLabels);
    } else {
      return null;
    }
  }

  protected Object v2e(final PDatabase graph, final PIdentifiable iRecord, final PVertex.DIRECTION iDirection, final String[] iLabels) {
    OElement rec = iRecord.getRecord();
    if (rec.isVertex()) {
      return rec.asVertex().get().getEdges(iDirection, iLabels);
    } else {
      return null;
    }
  }

  protected Object e2v(final PDatabase graph, final PIdentifiable iRecord, final PVertex.DIRECTION iDirection, final String[] iLabels) {
    OElement rec = iRecord.getRecord();
    if (rec.isEdge()) {
      if (iDirection == PVertex.DIRECTION.BOTH) {
        List results = new ArrayList();
        results.add(rec.asEdge().get().getVertex(PVertex.DIRECTION.OUT));
        results.add(rec.asEdge().get().getVertex(PVertex.DIRECTION.IN));
        return results;
      }
      return rec.asEdge().get().getVertex(iDirection);
    } else {
      return null;
    }
  }
}