package com.arcadedb.sql.executor;

import com.arcadedb.database.Document;
import com.arcadedb.database.Identifiable;
import com.arcadedb.sql.parser.MatchPathItem;
import com.arcadedb.sql.parser.Rid;
import com.arcadedb.sql.parser.WhereClause;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Created by luigidellaquila on 15/10/16.
 */
public class MatchReverseEdgeTraverser extends MatchEdgeTraverser {

  private final String startingPointAlias;
  private final String endPointAlias;

  public MatchReverseEdgeTraverser(Result lastUpstreamRecord, EdgeTraversal edge) {
    super(lastUpstreamRecord, edge);
    this.startingPointAlias = edge.edge.in.alias;
    this.endPointAlias = edge.edge.out.alias;
  }

  protected String targetClassName(MatchPathItem item, CommandContext iCommandContext) {
    return edge.getLeftClass();
  }

  protected String targetClusterName(MatchPathItem item, CommandContext iCommandContext) {
    return edge.getLeftCluster();
  }

  protected Rid targetRid(MatchPathItem item, CommandContext iCommandContext) {
    return edge.getLeftRid();
  }

  protected WhereClause getTargetFilter(MatchPathItem item) {
    return edge.getLeftFilter();
  }

  @Override
  protected Iterable<ResultInternal> traversePatternEdge(Identifiable startingPoint, CommandContext iCommandContext) {

    Object qR = this.item.getMethod().executeReverse(startingPoint, iCommandContext);
    if (qR == null) {
      return Collections.emptyList();
    }
    if (qR instanceof ResultInternal) {
      return Collections.singleton((ResultInternal) qR);
    }
    if (qR instanceof Document) {
      return Collections.singleton(new ResultInternal((Document) qR));
    }
    if (qR instanceof Iterable) {
      Iterable iterable = (Iterable) qR;
      List<ResultInternal> result = new ArrayList<>();
      for (Object o : iterable) {
        if (o instanceof Document) {
          result.add(new ResultInternal((Document) o));
        } else if (o instanceof ResultInternal) {
          result.add((ResultInternal) o);
        } else if (o == null) {
          continue;
        } else {
          throw new UnsupportedOperationException();
        }
      }
      return result;
    }
    return Collections.EMPTY_LIST;
  }

  @Override
  protected String getStartingPointAlias() {
    return this.startingPointAlias;
  }

  @Override
  protected String getEndpointAlias() {
    return endPointAlias;
  }

}
