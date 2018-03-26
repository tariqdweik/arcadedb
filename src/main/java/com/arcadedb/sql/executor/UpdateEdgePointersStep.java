package com.arcadedb.sql.executor;

import com.arcadedb.database.PIdentifiable;
import com.arcadedb.database.PRecord;
import com.arcadedb.exception.PTimeoutException;
import com.arcadedb.graph.PEdge;

import java.util.Map;
import java.util.Optional;

/**
 * after an update of an edge, this step updates edge pointers on vertices to make the graph consistent again
 */
public class UpdateEdgePointersStep extends AbstractExecutionStep {

  public UpdateEdgePointersStep(OCommandContext ctx, boolean profilingEnabled) {
    super(ctx, profilingEnabled);
  }

  @Override
  public OResultSet syncPull(OCommandContext ctx, int nRecords) throws PTimeoutException {
    OResultSet upstream = getPrev().get().syncPull(ctx, nRecords);
    return new OResultSet() {
      @Override
      public boolean hasNext() {
        return upstream.hasNext();
      }

      @Override
      public OResult next() {
        OResult result = upstream.next();
        if (result instanceof OResultInternal) {
          handleUpdateEdge((PRecord) result.getElement().get().getRecord());
        }
        return result;
      }

      private void updateIn(OResult item) {

      }

      private void updateOut(OResult item) {

      }

      @Override
      public void close() {
        upstream.close();
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

  @Override
  public String prettyPrint(int depth, int indent) {
    String spaces = OExecutionStepInternal.getIndent(depth, indent);
    StringBuilder result = new StringBuilder();
    result.append(spaces);
    result.append("+ UPDATE EDGE POINTERS");
    return result.toString();
  }

  /**
   * handles vertex consistency after an UPDATE EDGE
   *
   * @param record the edge record
   */
  private void handleUpdateEdge(PRecord record) {
    Object currentOut = record.get("out");
    Object currentIn = record.get("in");

    throw new UnsupportedOperationException();
//    Object prevOut = record.getOriginalValue("out");
//    Object prevIn = record.getOriginalValue("in");
//
//    validateOutInForEdge(record, currentOut, currentIn);
//
//    changeVertexEdgePointer(record, (PIdentifiable) prevIn, (PIdentifiable) currentIn, "in");
//    changeVertexEdgePointer(record, (PIdentifiable) prevOut, (PIdentifiable) currentOut, "out");
  }

  /**
   * updates old and new vertices connected to an edge after out/in update on the edge itself
   *
   * @param edge          the edge
   * @param prevVertex    the previously connected vertex
   * @param currentVertex the currently connected vertex
   * @param direction     the direction ("out" or "in")
   */
  private void changeVertexEdgePointer(PEdge edge, PIdentifiable prevVertex, PIdentifiable currentVertex, String direction) {
//    if (prevVertex != null && !prevVertex.equals(currentVertex)) {
//      String edgeClassName = edge.getClassName();
//      if (edgeClassName.equalsIgnoreCase("E")) {
//        edgeClassName = "";
//      }
//      String vertexFieldName = direction + "_" + edgeClassName;
//      ODocument prevOutDoc = ((PIdentifiable) prevVertex).getRecord();
//      ORidBag prevBag = prevOutDoc.field(vertexFieldName);
//      if (prevBag != null) {
//        prevBag.remove(edge);
//        prevOutDoc.save();
//      }
//
//      ODocument currentVertexDoc = ((PIdentifiable) currentVertex).getRecord();
//      ORidBag currentBag = currentVertexDoc.field(vertexFieldName);
//      if (currentBag == null) {
//        currentBag = new ORidBag();
//        currentVertexDoc.field(vertexFieldName, currentBag);
//      }
//      currentBag.add(edge);
//    }
  }

  private void validateOutInForEdge(PRecord record, Object currentOut, Object currentIn) {
//    if (!isRecordInstanceOf(currentOut, "V")) {
//      throw new PCommandExecutionException("Error updating edge: 'out' is not a vertex - " + currentOut + "");
//    }
//    if (!isRecordInstanceOf(currentIn, "V")) {
//      throw new PCommandExecutionException("Error updating edge: 'in' is not a vertex - " + currentIn + "");
//    }
  }

  /**
   * checks if an object is an PIdentifiable and an instance of a particular (schema) class
   *
   * @param iRecord     The record object
   * @param orientClass The schema class
   *
   * @return
   */
  private boolean isRecordInstanceOf(Object iRecord, String orientClass) {
    if (iRecord == null) {
      return false;
    }
    if (!(iRecord instanceof PIdentifiable)) {
      return false;
    }
    PRecord record = ((PIdentifiable) iRecord).getRecord();
    if (iRecord == null) {
      return false;
    }
    return (record.getType().equals(orientClass));
  }
}
