/*
 * Copyright (c) 2018 - Arcade Analytics LTD (https://arcadeanalytics.com)
 */

package org.apache.tinkerpop.gremlin.arcadedb.structure.io.graphson;

import com.arcadedb.database.Database;
import com.arcadedb.database.RID;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.io.graphson.AbstractObjectDeserializer;
import org.apache.tinkerpop.gremlin.structure.io.graphson.GraphSONTokens;
import org.apache.tinkerpop.gremlin.structure.util.detached.DetachedEdge;
import org.apache.tinkerpop.gremlin.structure.util.detached.DetachedVertex;
import org.apache.tinkerpop.shaded.jackson.databind.JsonDeserializer;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

import static org.apache.tinkerpop.gremlin.arcadedb.structure.io.ArcadeIoRegistry.isRID;
import static org.apache.tinkerpop.gremlin.arcadedb.structure.io.ArcadeIoRegistry.newRID;

/**
 * Created by Enrico Risa on 06/09/2017.
 */
public class ArcadeGraphSONV3 extends ArcadeGraphSON {

  public static ArcadeGraphSONV3 INSTANCE = new ArcadeGraphSONV3();

  protected static final Map<Class, String> TYPES = Collections.unmodifiableMap(new LinkedHashMap<Class, String>() {
    {
      put(RID.class, "RID");
    }
  });

  public ArcadeGraphSONV3() {
    super("arcade-graphson-v3");
    addSerializer(RID.class, new RIDJacksonSerializer());
    addDeserializer(RID.class, new RIDJacksonDeserializer());
    addDeserializer(Edge.class, new EdgeJacksonDeserializer());
    addDeserializer(Vertex.class, new VertexJacksonDeserializer());
    addDeserializer(Map.class, (JsonDeserializer) new RIDJacksonDeserializer());
  }

  @Override
  public Map<Class, String> getTypeDefinitions() {
    return TYPES;
  }

  /**
   * Created by Enrico Risa on 06/09/2017.
   */
  public static class EdgeJacksonDeserializer extends AbstractObjectDeserializer<Edge> {

    public EdgeJacksonDeserializer() {
      super(Edge.class);
    }

    @SuppressWarnings({ "unchecked", "rawtypes" })
    @Override
    public Edge createObject(final Map<String, Object> edgeData) {
      return new DetachedEdge(newRID((Database) edgeData.get("database"), edgeData.get(GraphSONTokens.ID)), edgeData.get(GraphSONTokens.LABEL).toString(),
          (Map) edgeData.get(GraphSONTokens.PROPERTIES), newRID((Database) edgeData.get("database"), edgeData.get(GraphSONTokens.OUT)),
          edgeData.get(GraphSONTokens.OUT_LABEL).toString(), newRID((Database) edgeData.get("database"), edgeData.get(GraphSONTokens.IN)),
          edgeData.get(GraphSONTokens.IN_LABEL).toString());
    }
  }

  /**
   * Created by Enrico Risa on 06/09/2017.
   */
  public static class VertexJacksonDeserializer extends AbstractObjectDeserializer<Vertex> {

    public VertexJacksonDeserializer() {
      super(Vertex.class);
    }

    @SuppressWarnings("unchecked")
    @Override
    public Vertex createObject(final Map<String, Object> vertexData) {
      return new DetachedVertex(newRID((Database) vertexData.get("database"), vertexData.get(GraphSONTokens.ID)),
          vertexData.get(GraphSONTokens.LABEL).toString(), (Map<String, Object>) vertexData.get(GraphSONTokens.PROPERTIES));
    }
  }

  final static class ORecordIdDeserializer extends AbstractObjectDeserializer<Object> {

    public ORecordIdDeserializer() {
      super(Object.class);
    }

    @Override
    public Object createObject(Map<String, Object> data) {

      if (isRID(data)) {
        return newRID(null, data);
      }
      return data;
    }

  }
}
