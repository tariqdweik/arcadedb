package org.apache.tinkerpop.gremlin.arcadedb;

import org.apache.commons.configuration.Configuration;
import org.apache.tinkerpop.gremlin.AbstractGraphProvider;
import org.apache.tinkerpop.gremlin.LoadGraphWith;
import org.apache.tinkerpop.gremlin.arcadedb.structure.*;
import org.apache.tinkerpop.gremlin.structure.Graph;

import java.io.File;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * Created by Enrico Risa on 30/07/2018.
 */
public class ArcadeGraphProvider extends AbstractGraphProvider {

  private static final Set<Class> IMPLEMENTATIONS = new HashSet<Class>() {{
    add(ArcadeEdge.class);
    add(ArcadeElement.class);
    add(ArcadeGraph.class);
    add(ArcadeGraphVariables.class);
    add(ArcadeProperty.class);
    add(ArcadeVertex.class);
    add(ArcadeVertexProperty.class);
  }};

  @Override
  public Map<String, Object> getBaseConfiguration(String graphName, Class<?> test, String testMethodName, LoadGraphWith.GraphData loadGraphWith) {

    final String directory = makeTestDirectory(graphName, test, testMethodName);

    return new HashMap<String, Object>() {{
      put(Graph.GRAPH, ArcadeGraph.class.getName());
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
