package org.apache.tinkerpop.gremlin.arcadedb.structure;

import com.arcadedb.database.ModifiableDocument;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Property;
import org.apache.tinkerpop.gremlin.structure.util.ElementHelper;

import java.util.*;

/**
 * Created by Enrico Risa on 30/07/2018.
 */
public abstract class ArcadeElement<T extends ModifiableDocument> implements Element {

  protected final T           baseElement;
  protected final ArcadeGraph graph;

  protected ArcadeElement(final ArcadeGraph graph, final T baseElement) {
    this.baseElement = baseElement;
    this.graph = graph;
  }

  @Override
  public String label() {
    return baseElement.getType();
  }

  @Override
  public <V> V value(final String key) throws NoSuchElementException {
    final V value = (V) baseElement.get(key);
    if (value == null)
      throw Property.Exceptions.propertyDoesNotExist(this, key);
    return value;
  }

  @Override
  public <V> Iterator<V> values(final String... propertyKeys) {
    final List<V> props;
    if (propertyKeys == null || propertyKeys.length == 0) {
      final Set<String> propNames = baseElement.getPropertyNames();
      props = new ArrayList<>(propNames.size());
      for (String p : propNames) {
        final V value = (V) baseElement.get(p);
        if (value != null)
          props.add(value);
      }
    } else {
      props = new ArrayList<>(propertyKeys.length);
      for (String p : propertyKeys) {
        final V value = (V) baseElement.get(p);
        if (value != null)
          props.add(value);
      }
    }
    return props.iterator();
  }

  @Override
  public void remove() {
    this.graph.tx().readWrite();
    this.graph.deleteElement(this);
  }

  @Override
  public Object id() {
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
