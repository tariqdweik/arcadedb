package com.arcadedb.sql.executor;

import com.arcadedb.database.PDocument;
import com.arcadedb.database.PIdentifiable;
import com.arcadedb.exception.PCommandExecutionException;
import com.arcadedb.sql.parser.*;

import java.util.*;

/**
 * Created by luigidellaquila on 14/10/16.
 */
public class MatchMultiEdgeTraverser extends MatchEdgeTraverser {
  public MatchMultiEdgeTraverser(OResult lastUpstreamRecord, EdgeTraversal edge) {
    super(lastUpstreamRecord, edge);
  }

  protected Iterable<OResultInternal> traversePatternEdge(PIdentifiable startingPoint, OCommandContext iCommandContext) {

    Iterable possibleResults = null;
    //    if (this.edge.edge.item.getFilter() != null) {
    //      String alias = this.edge.edge.item.getFilter().getAlias();
    //      Object matchedNodes = iCommandContext.getVariable(MatchPrefetchStep.PREFETCHED_MATCH_ALIAS_PREFIX + alias);
    //      if (matchedNodes != null) {
    //        if (matchedNodes instanceof Iterable) {
    //          possibleResults = (Iterable) matchedNodes;
    //        } else {
    //          possibleResults = Collections.singleton(matchedNodes);
    //        }
    //      }
    //    }

    MultiMatchPathItem item = (MultiMatchPathItem) this.item;
    List<OResultInternal> result = new ArrayList<>();

    List<Object> nextStep = new ArrayList<>();
    nextStep.add(startingPoint);

    Object oldCurrent = iCommandContext.getVariable("$current");
    for (MatchPathItem sub : item.getItems()) {
      List<OResultInternal> rightSide = new ArrayList<>();
      for (Object o : nextStep) {
        WhereClause whileCond = sub.getFilter() == null ? null : sub.getFilter().getWhileCondition();

        MethodCall method = sub.getMethod();
        if (sub instanceof MatchPathItemFirst) {
          method = ((MatchPathItemFirst) sub).getFunction().toMethod();
        }

        if (whileCond != null) {
          Object current = o;
          if (current instanceof OResult) {
            current = ((OResult) current).getElement().orElse(null);
          }
          MatchEdgeTraverser subtraverser = new MatchEdgeTraverser(null, sub);
          subtraverser.executeTraversal(iCommandContext, sub, (PIdentifiable) current, 0, null).forEach(x -> rightSide.add(x));

        } else {
          iCommandContext.setVariable("$current", o);
          Object nextSteps = method.execute(o, possibleResults, iCommandContext);
          if (nextSteps instanceof Collection) {
            ((Collection) nextSteps).stream().map(x -> toOResultInternal(x)).filter(Objects::nonNull)
                .forEach(i -> rightSide.add((OResultInternal) i));
          } else if (nextSteps instanceof PDocument) {
            rightSide.add(new OResultInternal((PDocument) nextSteps));
          } else if (nextSteps instanceof OResultInternal) {
            rightSide.add((OResultInternal) nextSteps);
          } else if (nextSteps instanceof Iterable) {
            for (Object step : (Iterable) nextSteps) {
              OResultInternal converted = toOResultInternal(step);
              if (converted != null) {
                rightSide.add(converted);
              }
            }
          } else if (nextSteps instanceof Iterator) {
            Iterator iterator = (Iterator) nextSteps;
            while (iterator.hasNext()) {
              OResultInternal converted = toOResultInternal(iterator.next());
              if (converted != null) {
                rightSide.add(converted);
              }
            }
          }
        }
      }
      nextStep = (List) rightSide;
      result = rightSide;
    }

    iCommandContext.setVariable("$current", oldCurrent);
    //    return (qR instanceof Iterable) ? (Iterable) qR : Collections.singleton((PIdentifiable) qR);
    return (Iterable) result;
  }

  private OResultInternal toOResultInternal(Object x) {
    if (x instanceof OResultInternal) {
      return (OResultInternal) x;
    }
    if (x instanceof PDocument) {
      return new OResultInternal((PDocument) x);
    }
    throw new PCommandExecutionException("Cannot execute traversal on " + x);
  }
}
