package org.apache.tinkerpop.gremlin.arcadedb.structure;

import com.arcadedb.database.ModifiableDocument;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.util.ElementHelper;

/**
 * Created by Enrico Risa on 30/07/2018.
 */
public abstract class ArcadeElement<T extends ModifiableDocument> implements Element {

  protected final T baseElement;
  protected final ArcadeGraph        graph;

  protected ArcadeElement(ArcadeGraph graph, T baseElement) {
    this.baseElement = baseElement;
    this.graph = graph;
  }

  @Override
  public String label() {
    this.graph.tx().readWrite();
    return baseElement.getType();
  }

  @Override
  public void remove() {
    this.graph.tx().readWrite();
    this.graph.deleteElement(this);
  }

  @Override
  public Object id() {
    this.graph.tx().readWrite();
    return baseElement.getIdentity();
  }

  @Override
  public Graph graph() {
    return this.graph;
  }

  @Override
  public boolean equals(final Object object) {
    return ElementHelper.areEqual(this, object);
  }

  @Override
  public int hashCode() {
    return ElementHelper.hashCode(this);
  }

  public T getBaseElement() {
    return baseElement;
  }
}
