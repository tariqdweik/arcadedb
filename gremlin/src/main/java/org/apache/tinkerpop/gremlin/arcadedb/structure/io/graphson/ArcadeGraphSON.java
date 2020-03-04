/*
 * Copyright (c) - Arcade Data LTD (https://arcadedata.com)
 */

package org.apache.tinkerpop.gremlin.arcadedb.structure.io.graphson;

import org.apache.tinkerpop.gremlin.structure.io.graphson.TinkerPopJacksonModule;

import java.util.Map;

/**
 * Created by Enrico Risa on 06/09/2017.
 */
public abstract class ArcadeGraphSON extends TinkerPopJacksonModule {

  public ArcadeGraphSON(String name) {
    super(name);
  }

  @Override
  public Map<Class, String> getTypeDefinitions() {
    return null;
  }

  @Override
  public String getTypeNamespace() {
    return "arcade";
  }
}
