/*
 * Copyright (c) - Arcade Data LTD (https://arcadedata.com)
 */

package com.arcadedb.sql.function.graph;

import com.arcadedb.database.Database;
import com.arcadedb.database.Document;
import com.arcadedb.database.Identifiable;
import com.arcadedb.graph.Edge;
import com.arcadedb.graph.Vertex;
import com.arcadedb.sql.executor.CommandContext;
import com.arcadedb.sql.executor.MultiValue;
import com.arcadedb.sql.executor.SQLEngine;
import com.arcadedb.sql.function.SQLFunctionConfigurableAbstract;
import com.arcadedb.utility.Callable;
import com.arcadedb.utility.FileUtils;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by luigidellaquila on 03/01/17.
 */
public abstract class SQLFunctionMove extends SQLFunctionConfigurableAbstract {
  public static final String NAME = "move";

  public SQLFunctionMove() {
    super(NAME, 1, 2);
  }

  public SQLFunctionMove(final String iName, final int iMin, final int iMax) {
    super(iName, iMin, iMax);
  }

  protected abstract Object move(final Database db, final Identifiable iRecord, final String[] iLabels);

  public String getSyntax() {
    return "Syntax error: " + name + "([<labels>])";
  }

  public Object execute(final Object iThis, final Identifiable iCurrentRecord, final Object iCurrentResult,
      final Object[] iParameters, final CommandContext iContext) {

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
        return move(iContext.getDatabase(), iArgument, labels);
      }
    }, iThis, iContext);

  }

  protected Object v2v(final Database graph, final Identifiable iRecord, final Vertex.DIRECTION iDirection,
      final String[] iLabels) {
    final Document rec = (Document) iRecord.getRecord();
    if (rec instanceof Vertex)
      return ((Vertex) rec).getVertices(iDirection, iLabels);
    return null;
  }

  protected Object v2e(final Database graph, final Identifiable iRecord, final Vertex.DIRECTION iDirection,
      final String[] iLabels) {
    Document rec = (Document) iRecord.getRecord();
    if (rec instanceof Vertex)
      return ((Vertex) rec).getEdges(iDirection, iLabels);
    return null;

  }

  protected Object e2v(final Database graph, final Identifiable iRecord, final Vertex.DIRECTION iDirection,
      final String[] iLabels) {
    Document rec = (Document) iRecord.getRecord();
    if (rec instanceof Edge) {
      if (iDirection == Vertex.DIRECTION.BOTH) {
        final List results = new ArrayList();
        results.add(((Edge) rec).getOutVertex());
        results.add(((Edge) rec).getInVertex());
        return results;
      }
      return ((Edge) rec).getVertex(iDirection);
    }

    return null;
  }
}