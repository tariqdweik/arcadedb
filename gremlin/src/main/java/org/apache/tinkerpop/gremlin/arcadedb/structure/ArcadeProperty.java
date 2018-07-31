package org.apache.tinkerpop.gremlin.arcadedb.structure;

import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Property;

import java.util.NoSuchElementException;
import java.util.Objects;

/**
 * Created by Enrico Risa on 30/07/2018.
 */
public class ArcadeProperty<T> implements Property<T> {

  protected final ArcadeElement element;
  protected final String        key;
  protected final ArcadeGraph   graph;
  protected       T             value;
  protected       boolean       removed = false;

  protected ArcadeProperty(final ArcadeElement element, final String key, final T value) {
    this.element = element;
    this.key = key;
    this.value = value;
    this.graph = (ArcadeGraph) element.graph();
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
  public Element element() {
    return element;
  }

  @Override
  public void remove() {
    if (this.removed)
      return;
    this.graph.tx().readWrite();
    element.baseElement.remove(key);
    element.baseElement.save();
    this.removed = true;
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o)
      return true;
    if (o == null || getClass() != o.getClass())
      return false;
    final ArcadeProperty<?> that = (ArcadeProperty<?>) o;
    return Objects.equals(element, that.element) && Objects.equals(key, that.key) && Objects.equals(graph, that.graph);
  }

  @Override
  public int hashCode() {
    return Objects.hash(graph, element, key);
  }
}
