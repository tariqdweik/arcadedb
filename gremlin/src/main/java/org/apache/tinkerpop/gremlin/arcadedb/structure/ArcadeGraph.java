package org.apache.tinkerpop.gremlin.arcadedb.structure;

import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.database.RID;
import com.arcadedb.engine.Bucket;
import com.arcadedb.engine.PaginatedFile;
import com.arcadedb.exception.RecordNotFoundException;
import com.arcadedb.graph.ModifiableEdge;
import com.arcadedb.graph.ModifiableVertex;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.EdgeType;
import com.arcadedb.schema.VertexType;
import com.arcadedb.sql.executor.ResultSet;
import com.arcadedb.utility.FileUtils;
import org.apache.commons.configuration.BaseConfiguration;
import org.apache.commons.configuration.Configuration;
import org.apache.tinkerpop.gremlin.process.computer.GraphComputer;
import org.apache.tinkerpop.gremlin.structure.*;
import org.apache.tinkerpop.gremlin.structure.util.ElementHelper;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;

import java.io.File;
import java.util.*;
import java.util.stream.Stream;

/**
 * Created by Enrico Risa on 30/07/2018.
 */

@Graph.OptIn(Graph.OptIn.SUITE_STRUCTURE_STANDARD)
@Graph.OptIn(Graph.OptIn.SUITE_STRUCTURE_INTEGRATE)
@Graph.OptIn(Graph.OptIn.SUITE_PROCESS_STANDARD)
@Graph.OptIn(Graph.OptIn.SUITE_PROCESS_COMPUTER)
@Graph.OptIn("org.apache.tinkerpop.gremlin.arcadedb.suite.ArcadeDebugSuite")
public class ArcadeGraph implements Graph {

  private final   ArcadeVariableFeatures graphVariables = new ArcadeVariableFeatures();
  private final   ArcadeGraphTransaction transaction;
  protected final Database               database;
  protected final BaseConfiguration      configuration  = new BaseConfiguration();

  private final static Iterator<Vertex> EMPTY_VERTICES = Collections.emptyIterator();
  private final static Iterator<Edge>   EMPTY_EDGES    = Collections.emptyIterator();

  protected Features features = new ArcadeGraphFeatures();

  protected ArcadeGraph(final Configuration configuration) {
    this.configuration.copy(configuration);
    final String directory = this.configuration.getString(CONFIG_DIRECTORY);
    DatabaseFactory factory = new DatabaseFactory(directory, PaginatedFile.MODE.READ_WRITE);

    if (!factory.exists())
      this.database = factory.create();
    else
      this.database = factory.open();

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
  public Vertex addVertex(final Object... keyValues) {
    ElementHelper.legalPropertyKeyValueArray(keyValues);
    if (ElementHelper.getIdValue(keyValues).isPresent())
      throw Vertex.Exceptions.userSuppliedIdsNotSupported();
    this.tx().readWrite();

    String label = ElementHelper.getLabelValue(keyValues).orElse(Vertex.DEFAULT_LABEL);

    if (!this.database.getSchema().existsType(label)) {
      this.database.getSchema().createVertexType(label);
    }
    final ModifiableVertex modifiableVertex = this.database.newVertex(label);
    final ArcadeVertex vertex = new ArcadeVertex(this, modifiableVertex);
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
  public Iterator<Vertex> vertices(final Object... vertexIds) {
    tx().readWrite();

    if (vertexIds.length == 0) {
      final Collection<DocumentType> types = this.database.getSchema().getTypes();
      final Set<Bucket> buckets = new HashSet<>();
      for (DocumentType t : types)
        if (t instanceof VertexType)
          buckets.addAll(t.getBuckets(true));

      if (buckets.isEmpty())
        return EMPTY_VERTICES;

      // BUILD THE QUERY
      final StringBuilder query = new StringBuilder("select from bucket:[");
      int i = 0;
      for (Bucket b : buckets) {
        if (i > 0)
          query.append(", ");
        query.append("`");
        query.append(b.getName());
        query.append("`");
        ++i;
      }
      query.append("]");

      final ResultSet resultset = this.database.query("sql", query.toString());
      return resultset.stream().map(result -> (Vertex) new ArcadeVertex(this, (ModifiableVertex) (result.toElement()).modify())).iterator();

    } else {

      ElementHelper.validateMixedElementIds(Vertex.class, vertexIds);
      return Stream.of(vertexIds).map(id -> {
        RID rid = null;
        if (id instanceof RID) {
          rid = (RID) id;
        }
        return rid;
      }).filter(rid -> rid != null).map(rid -> {
        try {
          return database.lookupByRID(rid, true);
        } catch (RecordNotFoundException e) {
          return null;
        }
      }).filter(record -> record != null).filter(com.arcadedb.graph.Vertex.class::isInstance)
          .map(record -> (Vertex) new ArcadeVertex(this, (ModifiableVertex) record.modify())).iterator();
    }
  }

  @Override
  public Iterator<Edge> edges(final Object... edgeIds) {
    tx().readWrite();

    if (edgeIds.length == 0) {

      final Collection<DocumentType> types = this.database.getSchema().getTypes();
      final Set<Bucket> buckets = new HashSet<>();
      for (DocumentType t : types)
        if (t instanceof EdgeType)
          buckets.addAll(t.getBuckets(true));

      if (buckets.isEmpty())
        return EMPTY_EDGES;

      // BUILD THE QUERY
      final StringBuilder query = new StringBuilder("select from bucket:[");
      int i = 0;
      for (Bucket b : buckets) {
        if (i > 0)
          query.append(", ");
        query.append("`");
        query.append(b.getName());
        query.append("`");
        ++i;
      }
      query.append("]");

      final ResultSet resultset = this.database.query("sql", query.toString());
      return resultset.stream().map(result -> (Edge) new ArcadeEdge(this, (ModifiableEdge) (result.toElement()).modify())).iterator();

    } else {
      ElementHelper.validateMixedElementIds(Edge.class, edgeIds);
      return Stream.of(edgeIds).map(id -> {
        RID rid;
        if (id instanceof RID)
          rid = (RID) id;
        else
          rid = null;
        return rid;
      }).filter(rid -> rid != null).map(rid -> database.lookupByRID(rid, false)).filter(com.arcadedb.graph.Edge.class::isInstance)
          .map(record -> (Edge) new ArcadeEdge(this, (ModifiableEdge) record.modify())).iterator();
    }
  }

  @Override
  public Transaction tx() {
    return transaction;
  }

  @Override
  public void close() {
    if (this.database != null)
      this.database.close();
  }

  public void drop() {
    if (this.database != null) {
      if (!this.database.isOpen())
        FileUtils.deleteRecursively(new File(this.database.getDatabasePath()));
      else
        this.database.drop();
    }
  }

  @Override
  public Variables variables() {
    throw Graph.Exceptions.variablesNotSupported();
  }

  @Override
  public Configuration configuration() {
    return configuration;
  }

  protected void deleteElement(final ArcadeElement element) {
    database.deleteRecord(element.getBaseElement().getRecord());
  }

  protected Database getDatabase() {
    return database;
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o)
      return true;
    if (o == null || getClass() != o.getClass())
      return false;
    ArcadeGraph that = (ArcadeGraph) o;
    return Objects.equals(database, that.database);
  }

  @Override
  public int hashCode() {
    return Objects.hash(database);
  }

  @Override
  public String toString() {
    return StringFactory.graphString(this, database.getName());
  }

  public static com.arcadedb.graph.Vertex.DIRECTION mapDirection(final Direction direction) {
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
