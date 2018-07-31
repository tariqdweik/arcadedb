package org.apache.tinkerpop.gremlin.arcadedb.structure;

import com.arcadedb.graph.ModifiableEdge;
import com.arcadedb.graph.ModifiableVertex;
import org.apache.tinkerpop.gremlin.structure.*;
import org.apache.tinkerpop.gremlin.structure.util.ElementHelper;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;

import java.util.Iterator;

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
  public Iterator<Edge> edges(final Direction direction, final String... edgeLabels) {
    if (edgeLabels.length == 0) {
      return IteratorUtils.stream(this.baseElement.getEdges(ArcadeGraph.mapDirection(direction)))
          .map((edge -> (Edge) new ArcadeEdge(this.graph, (ModifiableEdge) edge.modify()))).iterator();
    } else {
      return IteratorUtils.stream(this.baseElement.getEdges(ArcadeGraph.mapDirection(direction), edgeLabels))
          .map((edge -> (Edge) new ArcadeEdge(this.graph, (ModifiableEdge) edge.modify()))).iterator();
    }
  }

  @Override
  public Iterator<Vertex> vertices(final Direction direction, final String... edgeLabels) {
    if (edgeLabels.length == 0) {
      return IteratorUtils.stream(this.baseElement.getVertices(ArcadeGraph.mapDirection(direction)))
          .map((vertex -> (Vertex) new ArcadeVertex(this.graph, (ModifiableVertex) vertex.modify()))).iterator();
    } else {
      return IteratorUtils.stream(this.baseElement.getVertices(ArcadeGraph.mapDirection(direction), edgeLabels))
          .map((vertex -> (Vertex) new ArcadeVertex(this.graph, (ModifiableVertex) vertex.modify()))).iterator();
    }
  }

  @Override
  public <V> Iterator<VertexProperty<V>> properties(final String... propertyKeys) {
    return (Iterator) baseElement.getPropertyNames().stream().filter(key -> ElementHelper.keyExists(key, propertyKeys))
        .map(key -> new ArcadeVertexProperty<>(this, key, (V) baseElement.get(key))).iterator();
  }

  @Override
  public String toString() {
    return StringFactory.vertexString(this);
  }
}
