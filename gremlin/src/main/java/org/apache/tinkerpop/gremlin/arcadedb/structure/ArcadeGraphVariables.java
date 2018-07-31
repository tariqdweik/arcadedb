package org.apache.tinkerpop.gremlin.arcadedb.structure;

import org.apache.tinkerpop.gremlin.structure.Graph;

import java.util.Optional;
import java.util.Set;

/**
 * Created by Enrico Risa on 30/07/2018.
 */
public class ArcadeGraphVariables implements Graph.Variables {

  private ArcadeGraph graph;

  public ArcadeGraphVariables(ArcadeGraph graph) {
    this.graph = graph;
  }

  @Override
  public Set<String> keys() {
    return null;
  }

  @Override
  public <R> Optional<R> get(String key) {
    return Optional.empty();
  }

  @Override
  public void set(String key, Object value) {

  }

  @Override
  public void remove(String key) {

  }


  public static class ArcadeVariableFeatures implements Graph.Features.VariableFeatures {

  }

}
