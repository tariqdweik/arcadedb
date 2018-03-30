package com.arcadedb.sql.executor;

import com.arcadedb.database.PIdentifiable;
import com.arcadedb.database.PRID;
import com.arcadedb.database.PRecord;
import com.arcadedb.exception.PCommandExecutionException;
import com.arcadedb.exception.PTimeoutException;
import com.arcadedb.graph.PEdge;
import com.arcadedb.graph.PVertex;
import com.arcadedb.sql.parser.Identifier;

import java.util.*;

/**
 * Created by luigidellaquila on 21/02/17.
 */
public class FetchEdgesFromToVerticesStep extends AbstractExecutionStep {
  private final Identifier targetClass;
  private final Identifier targetCluster;
  private final String     fromAlias;
  private final String     toAlias;

  //operation stuff

  //iterator of FROM vertices
  Iterator        fromIter;
  //iterator of edges on current from
  Iterator<PEdge> currentFromEdgesIter;
  Iterator        toIterator;

  Set<PRID> toList = new HashSet<>();
  private boolean inited = false;

  private PEdge nextEdge = null;

  public FetchEdgesFromToVerticesStep(String fromAlias, String toAlias, Identifier targetClass, Identifier targetCluster,
      OCommandContext ctx, boolean profilingEnabled) {
    super(ctx, profilingEnabled);
    this.targetClass = targetClass;
    this.targetCluster = targetCluster;
    this.fromAlias = fromAlias;
    this.toAlias = toAlias;
  }

  @Override
  public OResultSet syncPull(OCommandContext ctx, int nRecords) throws PTimeoutException {
    getPrev().ifPresent(x -> x.syncPull(ctx, nRecords));
    init();
    return new OResultSet() {
      int currentBatch = 0;

      @Override
      public boolean hasNext() {
        return (currentBatch < nRecords && nextEdge != null);
      }

      @Override
      public OResult next() {
        if (!hasNext()) {
          throw new IllegalStateException();
        }
        PEdge edge = nextEdge;
        fetchNextEdge();
        OResultInternal result = new OResultInternal();
        result.setElement(edge);
        return result;
      }

      @Override
      public void close() {
        if (fromIter instanceof OResultSet) {
          ((OResultSet) fromIter).close();
        }
      }

      @Override
      public Optional<OExecutionPlan> getExecutionPlan() {
        return null;
      }

      @Override
      public Map<String, Long> getQueryStats() {
        return null;
      }
    };
  }

  private PVertex asVertex(Object currentFrom) {
    if (currentFrom instanceof PRID) {
      currentFrom = ((PRID) currentFrom).getRecord();
    }
    if (currentFrom instanceof OResult) {
      return ((OResult) currentFrom).getVertex().orElse(null);
    }
    if (currentFrom instanceof PVertex) {
      return (PVertex) currentFrom;
    }
    return null;
  }

  private void init() {
    synchronized (this) {
      if (this.inited) {
        return;
      }
      inited = true;
    }

    Object fromValues = null;

    fromValues = ctx.getVariable(fromAlias);
    if (fromValues instanceof Iterable && !(fromValues instanceof PIdentifiable)) {
      fromValues = ((Iterable) fromValues).iterator();
    } else if (!(fromValues instanceof Iterator)) {
      fromValues = Collections.singleton(fromValues).iterator();
    }

    Object toValues = null;

    toValues = ctx.getVariable(toAlias);
    if (toValues instanceof Iterable && !(toValues instanceof PIdentifiable)) {
      toValues = ((Iterable) toValues).iterator();
    } else if (!(toValues instanceof Iterator)) {
      toValues = Collections.singleton(toValues).iterator();
    }

    fromIter = (Iterator) fromValues;

    Iterator toIter = (Iterator) toValues;

    while (toIter != null && toIter.hasNext()) {
      Object elem = toIter.next();
      if (elem instanceof OResult) {
        elem = ((OResult) elem).toElement();
      }
      if (elem instanceof PIdentifiable && !(elem instanceof PRecord)) {
        elem = ((PIdentifiable) elem).getRecord();
      }
      if (!(elem instanceof PRecord)) {
        throw new PCommandExecutionException("Invalid vertex: " + elem);
      }
      if (elem instanceof PVertex) {
        toList.add(((PVertex) elem).getIdentity());
      }
    }

    toIterator = toList.iterator();

    fetchNextEdge();
  }

  private void fetchNextEdge() {
    this.nextEdge = null;
    while (true) {
      while (this.currentFromEdgesIter == null || !this.currentFromEdgesIter.hasNext()) {
        if (this.fromIter == null) {
          return;
        }
        if (this.fromIter.hasNext()) {
          Object from = fromIter.next();
          if (from instanceof OResult) {
            from = ((OResult) from).toElement();
          }
          if (from instanceof PIdentifiable && !(from instanceof PRecord)) {
            from = ((PIdentifiable) from).getRecord();
          }
          if (from instanceof PVertex) {

            currentFromEdgesIter = ((PVertex) from).getEdges(PVertex.DIRECTION.OUT);
          } else {
            throw new PCommandExecutionException("Invalid vertex: " + from);
          }
        } else {
          return;
        }
      }
      PEdge edge = this.currentFromEdgesIter.next();
      if (toList == null || toList.contains(edge.getIn().getIdentity())) {
        if (matchesClass(edge) && matchesCluster(edge)) {
          this.nextEdge = edge;
          return;
        }
      }
    }
  }

  private boolean matchesCluster(PEdge edge) {
    if (targetCluster == null) {
      return true;
    }
    int clusterId = edge.getIdentity().getBucketId();
    String clusterName = ctx.getDatabase().getSchema().getBucketById(clusterId).getName();
    return clusterName.equals(targetCluster.getStringValue());
  }

  private boolean matchesClass(PEdge edge) {
    if (targetClass == null) {
      return true;
    }
    return edge.getType().equals(targetClass.getStringValue());
  }

  @Override
  public String prettyPrint(int depth, int indent) {
    String spaces = OExecutionStepInternal.getIndent(depth, indent);
    String result = spaces + "+ FOR EACH x in " + fromAlias + "\n";
    result += spaces + "    FOR EACH y in " + toAlias + "\n";
    result += spaces + "       FETCH EDGES FROM x TO y";
    if (targetClass != null) {
      result += "\n" + spaces + "       (target class " + targetClass + ")";
    }
    if (targetCluster != null) {
      result += "\n" + spaces + "       (target cluster " + targetCluster + ")";
    }
    return result;
  }
}
