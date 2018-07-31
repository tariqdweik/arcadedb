package org.apache.tinkerpop.gremlin.arcadedb.structure;

import com.arcadedb.graph.ModifiableEdge;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Property;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.util.ElementHelper;

import java.util.Iterator;

/**
 * Created by Enrico Risa on 30/07/2018.
 */
public class ArcadeEdge extends ArcadeElement<ModifiableEdge> implements Edge {

  protected ArcadeEdge(ArcadeGraph graph, ModifiableEdge baseElement) {
    super(graph, baseElement);
  }

  @Override
  public Iterator<Vertex> vertices(Direction direction) {
    return null;
  }

  @Override
  public <V> Property<V> property(String key, V value) {

    ElementHelper.validateProperty(key, value);
    this.graph.tx().readWrite();
    baseElement.set(key, value);
    baseElement.save();
    return new ArcadeProperty<>(this, key, value);
  }

  @Override
  public <V> Iterator<Property<V>> properties(String... propertyKeys) {
    return (Iterator) baseElement.getPropertyNames().stream().filter(key -> ElementHelper.keyExists(key, propertyKeys))
        .map(key -> new ArcadeProperty<>(this, key, (V) baseElement.get(key))).iterator();
  }
}
