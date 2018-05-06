package com.arcadedb.sql.function.graph;

import com.arcadedb.database.PDatabase;
import com.arcadedb.database.PIdentifiable;
import com.arcadedb.graph.PVertex;

import java.util.Collections;

/**
 * Created by luigidellaquila on 03/01/17.
 */
public class OSQLFunctionIn extends OSQLFunctionMoveFiltered {
  public static final String NAME = "in";

  public OSQLFunctionIn() {
    super(NAME, 0, -1);
  }

  @Override
  protected Object move(final PDatabase graph, final PIdentifiable iRecord, final String[] iLabels) {
    return v2v(graph, iRecord, PVertex.DIRECTION.IN, iLabels);
  }

  protected Object move(final PDatabase graph, final PIdentifiable iRecord, final String[] iLabels,
      Iterable<PIdentifiable> iPossibleResults) {
    if (iPossibleResults == null) {
      return v2v(graph, iRecord, PVertex.DIRECTION.IN, iLabels);
    }

    if (!iPossibleResults.iterator().hasNext()) {
      return Collections.emptyList();
    }

    return v2v(graph, iRecord, PVertex.DIRECTION.IN, iLabels);
  }

  private Object fetchFromIndex(PDatabase graph, PIdentifiable iFrom, Iterable<PIdentifiable> iTo, String[] iEdgeTypes) {
    throw new UnsupportedOperationException();
//    String edgeClassName = null;
//    if (iEdgeTypes == null) {
//      edgeClassName = "E";
//    } else if (iEdgeTypes.length == 1) {
//      edgeClassName = iEdgeTypes[0];
//    } else {
//      return null;
//    }
//    OClass edgeClass = graph.getMetadata().getSchema().getClass(edgeClassName);
//    if (edgeClass == null) {
//      return null;
//    }
//    Set<OIndex<?>> indexes = edgeClass.getInvolvedIndexes("in", "out");
//    if (indexes == null || indexes.size() == 0) {
//      return null;
//    }
//    OIndex index = indexes.iterator().next();
//
//    OMultiCollectionIterator<PVertex> result = new OMultiCollectionIterator<PVertex>();
//    for (PIdentifiable to : iTo) {
//      OCompositeKey key = new OCompositeKey(iFrom, to);
//      Object indexResult = index.get(key);
//      if (indexResult instanceof PIdentifiable) {
//        indexResult = Collections.singleton(indexResult);
//      }
//      Set<PIdentifiable> identities = new HashSet<PIdentifiable>();
//      for (PIdentifiable edge : ((Iterable<OEdge>) indexResult)) {
//        identities.add((PIdentifiable) ((ODocument) edge.getRecord()).rawField("out"));
//      }
//      result.add(identities);
//    }
//
//    return result;
  }

}
