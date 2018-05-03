package com.arcadedb.sql.executor;

import com.arcadedb.database.PDocument;
import com.arcadedb.database.PIdentifiable;
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

  public MatchReverseEdgeTraverser(OResult lastUpstreamRecord, EdgeTraversal edge) {
    super(lastUpstreamRecord, edge);
    this.startingPointAlias = edge.edge.in.alias;
    this.endPointAlias = edge.edge.out.alias;
  }

  protected String targetClassName(MatchPathItem item, OCommandContext iCommandContext) {
    return edge.getLeftClass();
  }

  protected String targetClusterName(MatchPathItem item, OCommandContext iCommandContext) {
    return edge.getLeftCluster();
  }

  protected Rid targetRid(MatchPathItem item, OCommandContext iCommandContext) {
    return edge.getLeftRid();
  }

  protected WhereClause getTargetFilter(MatchPathItem item) {
    return edge.getLeftFilter();
  }

  @Override
  protected Iterable<OResultInternal> traversePatternEdge(PIdentifiable startingPoint, OCommandContext iCommandContext) {

    Object qR = this.item.getMethod().executeReverse(startingPoint, iCommandContext);
    if (qR == null) {
      return Collections.emptyList();
    }
    if (qR instanceof OResultInternal) {
      return Collections.singleton((OResultInternal) qR);
    }
    if (qR instanceof PDocument) {
      return Collections.singleton(new OResultInternal((PDocument) qR));
    }
    if (qR instanceof Iterable) {
      Iterable iterable = (Iterable) qR;
      List<OResultInternal> result = new ArrayList<>();
      for (Object o : iterable) {
        if (o instanceof PDocument) {
          result.add(new OResultInternal((PDocument) o));
        } else if (o instanceof OResultInternal) {
          result.add((OResultInternal) o);
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
