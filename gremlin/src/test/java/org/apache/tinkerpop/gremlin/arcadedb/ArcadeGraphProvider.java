package org.apache.tinkerpop.gremlin.arcadedb;

import org.apache.commons.configuration.Configuration;
import org.apache.tinkerpop.gremlin.AbstractGraphProvider;
import org.apache.tinkerpop.gremlin.LoadGraphWith;
import org.apache.tinkerpop.gremlin.arcadedb.structure.*;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.TransactionTest;
import org.apache.tinkerpop.gremlin.structure.VertexTest;
import org.junit.AssumptionViolatedException;

import java.io.File;
import java.util.*;

import static org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils.asList;
import static org.junit.Assume.assumeFalse;

/**
 * Created by Enrico Risa on 30/07/2018.
 */
public class ArcadeGraphProvider extends AbstractGraphProvider {
  protected static final Map<Class<?>, List<String>> IGNORED_TESTS;

  static {
    IGNORED_TESTS = new HashMap<>();
    IGNORED_TESTS.put(TransactionTest.class, asList("shouldExecuteWithCompetingThreads"));
    IGNORED_TESTS.put(VertexTest.BasicVertexTest.class, Arrays.asList("shouldNotGetConcurrentModificationException"));
  }

  private static final Set<Class> IMPLEMENTATIONS = new HashSet<Class>() {{
    add(ArcadeEdge.class);
    add(ArcadeElement.class);
    add(ArcadeGraph.class);
    add(ArcadeVariableFeatures.class);
    add(ArcadeProperty.class);
    add(ArcadeVertex.class);
    add(ArcadeVertexProperty.class);
  }};

  @Override
  public Map<String, Object> getBaseConfiguration(String graphName, Class<?> test, String testMethodName, LoadGraphWith.GraphData loadGraphWith) {
    if (IGNORED_TESTS.containsKey(test) && IGNORED_TESTS.get(test).contains(testMethodName))
      throw new AssumptionViolatedException("Ignored Test");

    if (testMethodName.contains("graphson"))
      throw new AssumptionViolatedException("graphson support not implemented");

    if (testMethodName.contains("gryo"))
      throw new AssumptionViolatedException("gryo support not implemented");

    final String directory = makeTestDirectory(graphName, test, testMethodName);

    return new HashMap<String, Object>() {{
      put(Graph.GRAPH, ArcadeGraph.class.getName());
      put("name", graphName);
      put(ArcadeGraph.CONFIG_DIRECTORY, directory);
    }};
  }

  @Override
  public void clear(Graph graph, Configuration configuration) throws Exception {

    if (graph != null)
      ((ArcadeGraph) graph).drop();

    if (configuration != null && configuration.containsKey(ArcadeGraph.CONFIG_DIRECTORY)) {
      // this is a non-in-sideEffects configuration so blow away the directory
      final File graphDirectory = new File(configuration.getString(ArcadeGraph.CONFIG_DIRECTORY));
      deleteDirectory(graphDirectory);
    }
  }

  @Override
  public Graph openTestGraph(Configuration config) {
    if ("readGraph".equals(config.getString("name")))
      // FIXME eventually ne need to get ride of this
      assumeFalse("there is some technical limitation in ArcadeDB that makes tests enter in an infinite loop when reading and writing to ArcadeDB", true);

    //FileUtils.deleteRecursively(new File(config.getString("gremlin.arcadedb.directory")));

    return super.openTestGraph(config);
  }

  @Override
  public Set<Class> getImplementations() {
    return IMPLEMENTATIONS;
  }

  protected String makeTestDirectory(final String graphName, final Class<?> test, final String testMethodName) {
    return this.getWorkingDirectory() + File.separator + cleanPathSegment(this.getClass().getSimpleName()) + File.separator + cleanPathSegment(
        test.getSimpleName()) + File.separator + cleanPathSegment(graphName) + File.separator + cleanParameters(cleanPathSegment(testMethodName));
  }

  public static String cleanPathSegment(final String toClean) {
    String cleaned = toClean.replaceAll("[.\\\\/,{}:*?\"<>|\\[\\]\\(\\)]", "");
    if (cleaned.length() == 0) {
      throw new IllegalStateException("Path segment " + toClean + " has not valid characters and is thus empty");
    } else {
      return cleaned;
    }
  }
}
