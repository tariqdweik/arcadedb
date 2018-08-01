package org.apache.tinkerpop.gremlin.arcadedb.structure;

import com.arcadedb.graph.ModifiableEdge;
import com.arcadedb.graph.ModifiableVertex;
import org.apache.commons.collections.iterators.ArrayIterator;
import org.apache.commons.collections.iterators.SingletonIterator;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Property;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.util.ElementHelper;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * Created by Enrico Risa on 30/07/2018.
 */
public class ArcadeEdge extends ArcadeElement<ModifiableEdge> implements Edge {

  protected ArcadeEdge(final ArcadeGraph graph, final ModifiableEdge baseElement) {
    super(graph, baseElement);
  }

  @Override
  public Vertex outVertex() {
    return new ArcadeVertex(graph, (ModifiableVertex) baseElement.getOutVertex().modify());
  }

  @Override
  public Vertex inVertex() {
    return new ArcadeVertex(graph, (ModifiableVertex) baseElement.getInVertex().modify());
  }

  @Override
  public Iterator<Vertex> vertices(final Direction direction) {
    switch (direction) {
    case IN:
      return new SingletonIterator(new ArcadeVertex(graph, (ModifiableVertex) baseElement.getInVertex().modify()));
    case OUT:
      return new SingletonIterator(new ArcadeVertex(graph, (ModifiableVertex) baseElement.getOutVertex().modify()));
    case BOTH:
      return new ArrayIterator(new Vertex[] { new ArcadeVertex(graph, (ModifiableVertex) baseElement.getOutVertex().modify()),
          new ArcadeVertex(graph, (ModifiableVertex) baseElement.getInVertex().modify()) });
    default:
      throw new IllegalArgumentException("Direction " + direction + " not supported");
    }
  }

  @Override
  public <V> Property<V> property(final String key, final V value) {
    ElementHelper.validateProperty(key, value);
    this.graph.tx().readWrite();
    baseElement.set(key, value);
    baseElement.save();
    return new ArcadeProperty<>(this, key, value);
  }

  @Override
  public <V> Property<V> property(final String key) {
    final V value = (V) baseElement.get(key);
    if (value != null)
      return new ArcadeProperty<>(this, key, value);
    return Property.empty();
  }

  @Override
  public <V> Iterator<Property<V>> properties(final String... propertyKeys) {
    final List<ArcadeProperty> props = new ArrayList<>();
    for (String p : baseElement.getPropertyNames()) {
      props.add(new ArcadeProperty<>(this, p, (V) baseElement.get(p)));
    }
    return (Iterator) props.iterator();
  }

  @Override
  public String toString() {
    return StringFactory.edgeString(this);
  }
}
