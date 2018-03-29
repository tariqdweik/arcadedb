package com.arcadedb.sql.executor;

import com.arcadedb.database.PIdentifiable;
import com.arcadedb.database.PRecord;
import com.arcadedb.exception.PCommandExecutionException;
import com.arcadedb.exception.PTimeoutException;
import com.arcadedb.graph.PEdge;
import com.arcadedb.graph.PVertex;
import com.arcadedb.sql.parser.Identifier;

import java.util.Collections;
import java.util.Iterator;
import java.util.Map;
import java.util.Optional;

/**
 * Created by luigidellaquila on 21/02/17.
 */
public class FetchEdgesToVerticesStep extends AbstractExecutionStep {
  private final String     toAlias;
  private final Identifier targetCluster;
  private final Identifier targetClass;

  private boolean inited = false;
  private Iterator       toIter;
  private PEdge          nextEdge;
  private Iterator<PEdge> currentToEdgesIter;

  public FetchEdgesToVerticesStep(String toAlias, Identifier targetClass, Identifier targetCluster, OCommandContext ctx, boolean profilingEnabled) {
    super(ctx, profilingEnabled);
    this.toAlias = toAlias;
    this.targetClass = targetClass;
    this.targetCluster = targetCluster;
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
        if (toIter instanceof OResultSet) {
          ((OResultSet) toIter).close();
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

  private void init() {
    synchronized (this) {
      if (this.inited) {
        return;
      }
      inited = true;
    }

    Object toValues = null;

    toValues = ctx.getVariable(toAlias);
    if (toValues instanceof Iterable && !(toValues instanceof PIdentifiable)) {
      toValues = ((Iterable) toValues).iterator();
    } else if (!(toValues instanceof Iterator)) {
      toValues = Collections.singleton(toValues).iterator();
    }

    this.toIter = (Iterator) toValues;

    fetchNextEdge();
  }

  private void fetchNextEdge() {
    this.nextEdge = null;
    while (true) {
      while (this.currentToEdgesIter == null || !this.currentToEdgesIter.hasNext()) {
        if (this.toIter == null) {
          return;
        }
        if (this.toIter.hasNext()) {
          Object from = toIter.next();
          if (from instanceof OResult) {
            from = ((OResult) from).toElement();
          }
          if (from instanceof PIdentifiable && !(from instanceof PRecord)) {
            from = ((PIdentifiable) from).getRecord();
          }
          if (from instanceof PVertex) {
            Iterable<PEdge> edges = ((PVertex) from).getEdges(PVertex.DIRECTION.IN);
            currentToEdgesIter = edges.iterator();
          } else {
            throw new PCommandExecutionException("Invalid vertex: " + from);
          }
        } else {
          return;
        }
      }
      PEdge edge = this.currentToEdgesIter.next();
      if (matchesClass(edge) && matchesCluster(edge)) {
        this.nextEdge = edge;
        return;
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
    String result = spaces + "+ FOR EACH x in " + toAlias + "\n";
    result += spaces + "       FETCH EDGES TO x";
    if (targetClass != null) {
      result += "\n" + spaces + "       (target class " + targetClass + ")";
    }
    if (targetCluster != null) {
      result += "\n" + spaces + "       (target cluster " + targetCluster + ")";
    }
    return result;
  }
}
