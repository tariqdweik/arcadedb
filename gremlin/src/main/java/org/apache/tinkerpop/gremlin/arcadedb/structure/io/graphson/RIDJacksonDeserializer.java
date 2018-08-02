/*
 * Copyright (c) 2018 - Arcade Analytics LTD (https://arcadeanalytics.com)
 */

package org.apache.tinkerpop.gremlin.arcadedb.structure.io.graphson;

import com.arcadedb.database.Database;
import com.arcadedb.database.RID;
import org.apache.tinkerpop.shaded.jackson.core.JsonParser;
import org.apache.tinkerpop.shaded.jackson.databind.DeserializationContext;
import org.apache.tinkerpop.shaded.jackson.databind.deser.std.StdDeserializer;

import java.io.IOException;

/**
 * Created by Enrico Risa on 06/09/2017.
 */
public class RIDJacksonDeserializer extends StdDeserializer<RID> {
  protected RIDJacksonDeserializer() {
    super(RID.class);
  }

  @Override
  public RID deserialize(final JsonParser jsonParser, final DeserializationContext deserializationContext) throws IOException {
    final String rid = deserializationContext.readValue(jsonParser, String.class);
    return new RID((Database) deserializationContext.getAttribute("database"), rid);
  }
}
