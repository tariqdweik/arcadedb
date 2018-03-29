package com.arcadedb.sql.executor;

import com.arcadedb.database.PRID;
import com.arcadedb.database.PRecord;
import com.arcadedb.sql.parser.PInteger;
import com.arcadedb.sql.parser.TraverseProjectionItem;
import com.arcadedb.sql.parser.WhereClause;

import java.util.*;

/**
 * Created by luigidellaquila on 26/10/16.
 */
public class BreadthFirstTraverseStep extends AbstractTraverseStep {

  public BreadthFirstTraverseStep(List<TraverseProjectionItem> projections, WhereClause whileClause, PInteger maxDepth,
      OCommandContext ctx, boolean profilingEnabled) {
    super(projections, whileClause, maxDepth, ctx, profilingEnabled);
  }

  @Override
  protected void fetchNextEntryPoints(OCommandContext ctx, int nRecords) {
    OResultSet nextN = getPrev().get().syncPull(ctx, nRecords);
    while (nextN.hasNext()) {
      while (nextN.hasNext()) {
        OResult item = toTraverseResult(nextN.next());
        if(item != null){
          ArrayDeque<PRID> stack = new ArrayDeque<PRID>();
          item.getIdentity().ifPresent(x -> stack.push(x));
          ((OResultInternal) item).setMetadata("$stack", stack);


          List<PRID> path = new ArrayList<>();
          path.add(item.getIdentity().get());
          ((OResultInternal) item).setMetadata("$path", path);

          if (item != null && item.isElement() && !traversed.contains(item.getElement().get().getIdentity())) {
            tryAddEntryPoint(item, ctx);

          }
        }
      }
      nextN = getPrev().get().syncPull(ctx, nRecords);
    }
  }

  private OResult toTraverseResult(OResult item) {
    OTraverseResult res = null;
    if (item instanceof OTraverseResult) {
      res = (OTraverseResult) item;
    } else if (item.isElement()) {
      res = new OTraverseResult();
      res.setElement(item.getElement().get());
      res.depth = 0;
      res.setMetadata("$depth", 0);
    } else {
      return null;
    }

    return res;
  }

  @Override
  protected void fetchNextResults(OCommandContext ctx, int nRecords) {
    if (!this.entryPoints.isEmpty()) {
      OTraverseResult item = (OTraverseResult) this.entryPoints.remove(0);
      this.results.add(item);
      for (TraverseProjectionItem proj : projections) {
        Object nextStep = proj.execute(item, ctx);
        if (this.maxDepth == null || this.maxDepth.getValue().intValue() > item.depth) {
          addNextEntryPoints(nextStep, item.depth + 1, (List<PRID>) item.getMetadata("$path"), ctx);
        }
      }
    }
  }

  private void addNextEntryPoints(Object nextStep, int depth, List<PRID> path, OCommandContext ctx) {
    if (nextStep instanceof PRecord) {
      addNextEntryPoints(((PRecord) nextStep), depth, path, ctx);
    } else if (nextStep instanceof Iterable) {
      addNextEntryPoints(((Iterable) nextStep).iterator(), depth, path, ctx);
    } else if (nextStep instanceof OResult) {
      addNextEntryPoints(((OResult) nextStep), depth, path, ctx);
    }
  }

  private void addNextEntryPoints(Iterator nextStep, int depth, List<PRID> path, OCommandContext ctx) {
    while (nextStep.hasNext()) {
      addNextEntryPoints(nextStep.next(), depth, path, ctx);
    }
  }

  private void addNextEntryPoints(PRecord nextStep, int depth, List<PRID> path, OCommandContext ctx) {
    if (this.traversed.contains(nextStep.getIdentity())) {
      return;
    }
    OTraverseResult res = new OTraverseResult();
    res.setElement(nextStep);
    res.depth = depth;
    res.setMetadata("$depth", depth);

    List<PRID> newPath = new ArrayList<>();
    newPath.addAll(path);
    newPath.add(res.getIdentity().get());
    res.setMetadata("$path", newPath);

    List reverseStack = new ArrayList();
    reverseStack.addAll(newPath);
    Collections.reverse(reverseStack);
    ArrayDeque newStack = new ArrayDeque();
    newStack.addAll(reverseStack);
    res.setMetadata("$stack", newStack);

    tryAddEntryPoint(res, ctx);

  }

  private void addNextEntryPoints(OResult nextStep, int depth, List<PRID> path, OCommandContext ctx) {
    if (!nextStep.isElement()) {
      return;
    }
    if (this.traversed.contains(nextStep.getElement().get().getIdentity())) {
      return;
    }
    if (nextStep instanceof OTraverseResult) {
      ((OTraverseResult) nextStep).depth = depth;
      ((OTraverseResult) nextStep).setMetadata("$depth", depth);

      List<PRID> newPath = new ArrayList<>();
      newPath.addAll(path);
      newPath.add(((OTraverseResult) nextStep).getIdentity().get());
      ((OTraverseResult) nextStep).setMetadata("$path", newPath);

      List reverseStack = new ArrayList();
      reverseStack.addAll(newPath);
      Collections.reverse(reverseStack);
      ArrayDeque newStack = new ArrayDeque();
      newStack.addAll(reverseStack);
      ((OTraverseResult) nextStep).setMetadata("$stack", newStack);

      tryAddEntryPoint(nextStep, ctx);
    } else {
      OTraverseResult res = new OTraverseResult();
      res.setElement(nextStep.getElement().get());
      res.depth = depth;
      res.setMetadata("$depth", depth);

      List<PRID> newPath = new ArrayList<>();
      newPath.addAll(path);
      newPath.add(((OTraverseResult) nextStep).getIdentity().get());
      ((OTraverseResult) nextStep).setMetadata("$path", newPath);

      List reverseStack = new ArrayList();
      reverseStack.addAll(newPath);
      Collections.reverse(reverseStack);
      ArrayDeque newStack = new ArrayDeque();
      newStack.addAll(reverseStack);
      ((OTraverseResult) nextStep).setMetadata("$stack", newStack);

      tryAddEntryPoint(res, ctx);
    }
  }

  private void tryAddEntryPoint(OResult res, OCommandContext ctx) {
    if (whileClause == null || whileClause.matchesFilters(res, ctx)) {
      this.entryPoints.add(res);
    }
    traversed.add(res.getElement().get().getIdentity());
  }

  @Override
  public String prettyPrint(int depth, int indent) {
    String spaces = OExecutionStepInternal.getIndent(depth, indent);
    StringBuilder result = new StringBuilder();
    result.append(spaces);
    result.append("+ BREADTH-FIRST TRAVERSE \n");
    if (whileClause != null) {
      result.append(spaces);
      result.append("WHILE " + whileClause.toString());
    }
    return result.toString();
  }
}
