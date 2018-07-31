package org.apache.tinkerpop.gremlin.arcadedb.structure;

import org.apache.tinkerpop.gremlin.structure.util.AbstractThreadLocalTransaction;
import org.apache.tinkerpop.gremlin.structure.util.TransactionException;

/**
 * Created by Enrico Risa on 30/07/2018.
 */
public class ArcadeGraphTransaction extends AbstractThreadLocalTransaction {

  private final ArcadeGraph graph;

  public ArcadeGraphTransaction(ArcadeGraph arcadeGraph) {
    super(arcadeGraph);
    graph = arcadeGraph;
  }

  @Override
  protected void doOpen() {
    graph.getDatabase().begin();
  }

  @Override
  protected void doCommit() throws TransactionException {
    graph.getDatabase().commit();
  }

  @Override
  protected void doRollback() throws TransactionException {
    graph.getDatabase().rollback();
  }

  @Override
  public boolean isOpen() {
    return graph.getDatabase().isTransactionActive();
  }
}
