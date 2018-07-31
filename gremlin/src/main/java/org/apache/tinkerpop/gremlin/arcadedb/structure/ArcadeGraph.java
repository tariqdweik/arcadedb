package org.apache.tinkerpop.gremlin.arcadedb.structure;

import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.database.RID;
import com.arcadedb.engine.PaginatedFile;
import com.arcadedb.graph.ModifiableEdge;
import com.arcadedb.graph.ModifiableVertex;
import com.arcadedb.schema.EdgeType;
import com.arcadedb.schema.VertexType;
import org.apache.commons.configuration.BaseConfiguration;
import org.apache.commons.configuration.Configuration;
import org.apache.tinkerpop.gremlin.process.computer.GraphComputer;
import org.apache.tinkerpop.gremlin.structure.*;
import org.apache.tinkerpop.gremlin.structure.util.ElementHelper;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;

import java.util.Iterator;
import java.util.stream.Stream;

/**
 * Created by Enrico Risa on 30/07/2018.
 */

@Graph.OptIn(Graph.OptIn.SUITE_STRUCTURE_STANDARD)
@Graph.OptIn(Graph.OptIn.SUITE_STRUCTURE_INTEGRATE)
@Graph.OptIn(Graph.OptIn.SUITE_PROCESS_STANDARD)
@Graph.OptIn(Graph.OptIn.SUITE_PROCESS_COMPUTER)
public class ArcadeGraph implements Graph {

  private final ArcadeGraphVariables   graphVariables;
  private final ArcadeGraphTransaction transaction;
  protected BaseConfiguration configuration = new BaseConfiguration();
  protected final Database database;

  protected Features features = new ArcadeGraphFeatures();

  protected ArcadeGraph(final Configuration configuration) {
    this.configuration.copy(configuration);
    final String directory = this.configuration.getString(CONFIG_DIRECTORY);
    DatabaseFactory factory = new DatabaseFactory(directory, PaginatedFile.MODE.READ_WRITE);
    if (!factory.exists()) {
      factory.create();
    }
    this.database = factory.open();
    this.graphVariables = new ArcadeGraphVariables(this);
    this.transaction = new ArcadeGraphTransaction(this);
  }

  public static final String CONFIG_DIRECTORY = "gremlin.arcadedb.directory";

  @Override
  public Features features() {
    return features;
  }

  public static ArcadeGraph open(final Configuration configuration) {
    if (null == configuration)
      throw Graph.Exceptions.argumentCanNotBeNull("configuration");
    if (!configuration.containsKey(CONFIG_DIRECTORY))
      throw new IllegalArgumentException(String.format("Arcade configuration requires that the %s be set", CONFIG_DIRECTORY));
    return new ArcadeGraph(configuration);
  }

  public static ArcadeGraph open(final String directory) {
    final Configuration config = new BaseConfiguration();
    config.setProperty(CONFIG_DIRECTORY, directory);
    return open(config);
  }

  @Override
  public Vertex addVertex(Object... keyValues) {

    ElementHelper.legalPropertyKeyValueArray(keyValues);
    if (ElementHelper.getIdValue(keyValues).isPresent())
      throw Vertex.Exceptions.userSuppliedIdsNotSupported();
    this.tx().readWrite();

    String label = ElementHelper.getLabelValue(keyValues).orElse(Vertex.DEFAULT_LABEL);

    if (!this.database.getSchema().existsType(label)) {
      this.database.getSchema().createVertexType(label);
    }
    ModifiableVertex modifiableVertex = this.database.newVertex(label);
    ArcadeVertex vertex = new ArcadeVertex(this, modifiableVertex);
    ElementHelper.attachProperties(vertex, keyValues);
    modifiableVertex.save();
    return vertex;
  }

  @Override
  public <C extends GraphComputer> C compute(Class<C> graphComputerClass) throws IllegalArgumentException {
    throw Graph.Exceptions.graphComputerNotSupported();
  }

  @Override
  public GraphComputer compute() throws IllegalArgumentException {
    throw Graph.Exceptions.graphComputerNotSupported();
  }

  @Override
  public Iterator<Vertex> vertices(Object... vertexIds) {
    this.tx().readWrite();
    if (vertexIds.length == 0) {
      return this.database.getSchema().getTypes().stream().filter(VertexType.class::isInstance)
          .flatMap((t) -> this.database.query("sql", String.format("select * from `%s`", t.getName())).vertexStream())
          .map(vertex -> (Vertex) new ArcadeVertex(this, (ModifiableVertex) vertex.modify())).iterator();
    } else {
      ElementHelper.validateMixedElementIds(Vertex.class, vertexIds);
      return Stream.of(vertexIds).map(id -> {
        RID rid = null;
        if (id instanceof RID) {
          rid = (RID) id;
        }
        return rid;
      }).map(rid -> database.lookupByRID(rid, true)).filter(Vertex.class::isInstance)
          .map(record -> (Vertex) new ArcadeVertex(this, (ModifiableVertex) record.modify())).iterator();
    }
  }

  @Override
  public Iterator<Edge> edges(Object... edgeIds) {
    this.tx().readWrite();
    if (edgeIds.length == 0) {
      return this.database.getSchema().getTypes().stream().filter(EdgeType.class::isInstance)
          .flatMap((t) -> this.database.query("sql", String.format("select * from `%s`", t.getName())).edgeStream())
          .map(vertex -> (Edge) new ArcadeEdge(this, (ModifiableEdge) vertex.modify())).iterator();
    } else {
      ElementHelper.validateMixedElementIds(Edge.class, edgeIds);
      return Stream.of(edgeIds).map(id -> {
        RID rid = null;
        if (id instanceof RID) {
          rid = (RID) id;
        }
        return rid;
      }).map(rid -> database.lookupByRID(rid, true)).filter(Edge.class::isInstance)
          .map(record -> (Edge) new ArcadeEdge(this, (ModifiableEdge) record.modify())).iterator();
    }
  }

  @Override
  public Transaction tx() {
    return transaction;
  }

  @Override
  public void close() throws Exception {
    this.tx().close();
    if (this.database != null) {
      this.database.close();
    }
  }

  @Override
  public Variables variables() {
    return graphVariables;
  }

  @Override
  public Configuration configuration() {
    return configuration;
  }

  protected void deleteElement(ArcadeElement element) {
    database.deleteRecord(element.getBaseElement().getRecord());
  }

  public class ArcadeGraphFeatures implements Features {

    protected GraphFeatures  graphFeatures  = new ArcadeGraphGraphFeatures();
    protected VertexFeatures vertexFeatures = new ArcadeVertexFeatures();
    protected EdgeFeatures   edgeFeatures   = new ArcadeEdgeFeatures();

    @Override
    public GraphFeatures graph() {
      return graphFeatures;
    }

    @Override
    public VertexFeatures vertex() {
      return vertexFeatures;
    }

    @Override
    public EdgeFeatures edge() {
      return edgeFeatures;
    }

    @Override
    public String toString() {
      return StringFactory.featureString(this);
    }

    public class ArcadeGraphGraphFeatures implements GraphFeatures {

      private VariableFeatures variableFeatures = new ArcadeGraphVariables.ArcadeVariableFeatures();

      @Override
      public boolean supportsConcurrentAccess() {
        return false;
      }

      @Override
      public boolean supportsComputer() {
        return false;
      }

      @Override
      public VariableFeatures variables() {
        return variableFeatures;
      }
    }

    public class ArcadeVertexFeatures implements VertexFeatures {

      @Override
      public VertexProperty.Cardinality getCardinality(String key) {
        return VertexProperty.Cardinality.single;
      }

      @Override
      public boolean supportsMetaProperties() {
        return false;
      }

      @Override
      public boolean supportsMultiProperties() {
        return false;
      }
    }

    public class ArcadeEdgeFeatures implements EdgeFeatures {

    }

  }

  protected Database getDatabase() {
    return database;
  }

  public static com.arcadedb.graph.Vertex.DIRECTION mapDirection(Direction direction) {

    switch (direction) {
    case OUT:
      return com.arcadedb.graph.Vertex.DIRECTION.OUT;
    case IN:
      return com.arcadedb.graph.Vertex.DIRECTION.IN;
    case BOTH:
      return com.arcadedb.graph.Vertex.DIRECTION.BOTH;
    }
    throw new IllegalArgumentException(String.format("Cannot get direction for argument %s", direction));
  }
}
