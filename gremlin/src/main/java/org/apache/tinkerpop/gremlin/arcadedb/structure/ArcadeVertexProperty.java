package org.apache.tinkerpop.gremlin.arcadedb.structure;

import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Property;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;
import org.apache.tinkerpop.gremlin.structure.util.ElementHelper;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;

import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 * Created by Enrico Risa on 30/07/2018.
 */
public class ArcadeVertexProperty<T> implements VertexProperty<T> {

  protected final String       key;
  protected final T            value;
  protected final ArcadeVertex vertex;

  protected ArcadeVertexProperty(final ArcadeVertex vertex, final String key, final T value) {
    this.vertex = vertex;
    this.key = key;
    this.value = value;
  }

  @Override
  public String key() {
    return key;
  }

  @Override
  public T value() throws NoSuchElementException {
    return value;
  }

  @Override
  public boolean isPresent() {
    return value != null;
  }

  @Override
  public Vertex element() {
    return vertex;
  }

  @Override
  public void remove() {
    graph().tx().readWrite();
    vertex.getBaseElement().remove(key);
    vertex.getBaseElement().save();
  }

  @Override
  public Object id() {
    return (long) (this.key.hashCode() + this.value.hashCode() + this.vertex.id().hashCode());
  }

  @Override
  public <V> Property<V> property(final String key, final V value) {
    throw new UnsupportedOperationException();
  }

  @Override
  public <U> Iterator<Property<U>> properties(final String... propertyKeys) {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean equals(final Object object) {
    return ElementHelper.areEqual(this, object);
  }

  @Override
  public int hashCode() {
    return ElementHelper.hashCode((Element) this);
  }

  @Override
  public String toString() {
    return StringFactory.propertyString(this);
  }
}
