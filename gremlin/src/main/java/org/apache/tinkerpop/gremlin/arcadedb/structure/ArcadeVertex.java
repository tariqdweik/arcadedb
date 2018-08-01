package org.apache.tinkerpop.gremlin.arcadedb.structure;

import com.arcadedb.graph.ModifiableEdge;
import com.arcadedb.graph.ModifiableVertex;
import org.apache.tinkerpop.gremlin.structure.*;
import org.apache.tinkerpop.gremlin.structure.util.ElementHelper;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

/**
 * Created by Enrico Risa on 30/07/2018.
 */
public class ArcadeVertex extends ArcadeElement<ModifiableVertex> implements Vertex {

  protected ArcadeVertex(final ArcadeGraph graph, final ModifiableVertex baseElement) {
    super(graph, baseElement);
  }

  @Override
  public Edge addEdge(final String label, final Vertex inVertex, final Object... keyValues) {
    if (null == inVertex)
      throw Graph.Exceptions.argumentCanNotBeNull("inVertex");
    ElementHelper.validateLabel(label);
    ElementHelper.legalPropertyKeyValueArray(keyValues);

    if (ElementHelper.getIdValue(keyValues).isPresent())
      throw Edge.Exceptions.userSuppliedIdsNotSupported();

    this.graph.tx().readWrite();
    ArcadeVertex vertex = (ArcadeVertex) inVertex;

    ElementHelper.getLabelValue(keyValues).orElse(Vertex.DEFAULT_LABEL);

    if (!this.graph.getDatabase().getSchema().existsType(label)) {
      this.graph.getDatabase().getSchema().createEdgeType(label);
    }

    ModifiableVertex baseElement = getBaseElement();

    com.arcadedb.graph.ModifiableEdge edge = baseElement.newEdge(label, vertex.getBaseElement(), true);
    ArcadeEdge arcadeEdge = new ArcadeEdge(graph, edge);
    ElementHelper.attachProperties(arcadeEdge, keyValues);
    edge.save();
    return arcadeEdge;
  }

  @Override
  public <V> VertexProperty<V> property(final VertexProperty.Cardinality cardinality, final String key, final V value, final Object... keyValues) {
    ElementHelper.validateProperty(key, value);
    if (ElementHelper.getIdValue(keyValues).isPresent())
      throw Vertex.Exceptions.userSuppliedIdsNotSupported();

    if (cardinality != VertexProperty.Cardinality.single)
      throw VertexProperty.Exceptions.multiPropertiesNotSupported();
    if (keyValues.length > 0)
      throw VertexProperty.Exceptions.metaPropertiesNotSupported();

    this.graph.tx().readWrite();

    baseElement.set(key, value);
    baseElement.save();
    return new ArcadeVertexProperty<>(this, key, value);
  }

  @Override
  public <V> VertexProperty<V> property(final String key, final V value) {
    ElementHelper.validateProperty(key, value);
    this.graph.tx().readWrite();
    baseElement.set(key, value);
    baseElement.save();
    return new ArcadeVertexProperty<>(this, key, value);
  }

  @Override
  public <V> VertexProperty<V> property(final String key) {
    final V value = (V) baseElement.get(key);
    if (value != null)
      return new ArcadeVertexProperty<>(this, key, value);
    return VertexProperty.empty();
  }

  @Override
  public <V> Iterator<VertexProperty<V>> properties(final String... propertyKeys) {
    final List<ArcadeVertexProperty> props;
    if (propertyKeys == null || propertyKeys.length == 0) {
      final Set<String> propNames = baseElement.getPropertyNames();
      props = new ArrayList<>(propNames.size());
      for (String p : propNames) {
        props.add(new ArcadeVertexProperty<>(this, p, (V) baseElement.get(p)));
      }
    } else {
      props = new ArrayList<>(propertyKeys.length);
      for (String p : propertyKeys) {
        props.add(new ArcadeVertexProperty<>(this, p, (V) baseElement.get(p)));
      }
    }
    return (Iterator) props.iterator();
  }

  @Override
  public Set<String> keys() {
    return baseElement.getPropertyNames();
  }

  @Override
  public Iterator<Edge> edges(final Direction direction, final String... edgeLabels) {
    final List<Edge> result = new ArrayList<>();

    if (edgeLabels.length == 0)
      for (com.arcadedb.graph.Edge edge : this.baseElement.getEdges(ArcadeGraph.mapDirection(direction)))
        result.add(new ArcadeEdge(this.graph, (ModifiableEdge) edge.modify()));
    else
      for (com.arcadedb.graph.Edge edge : this.baseElement.getEdges(ArcadeGraph.mapDirection(direction), edgeLabels))
        result.add(new ArcadeEdge(this.graph, (ModifiableEdge) edge.modify()));

    return result.iterator();
  }

  @Override
  public Iterator<Vertex> vertices(final Direction direction, final String... edgeLabels) {
    final List<Vertex> result = new ArrayList<>();

    if (edgeLabels.length == 0)
      for (com.arcadedb.graph.Vertex vertex : this.baseElement.getVertices(ArcadeGraph.mapDirection(direction)))
        result.add(new ArcadeVertex(this.graph, (ModifiableVertex) vertex.modify()));
    else
      for (com.arcadedb.graph.Vertex vertex : this.baseElement.getVertices(ArcadeGraph.mapDirection(direction), edgeLabels))
        result.add(new ArcadeVertex(this.graph, (ModifiableVertex) vertex.modify()));

    return result.iterator();
  }

  @Override
  public String toString() {
    return StringFactory.vertexString(this);
  }
}
