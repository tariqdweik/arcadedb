/*
 * Copyright (c) - Arcade Data LTD (https://arcadedata.com)
 */

package org.apache.tinkerpop.gremlin.arcadedb.structure.io.graphson;

import com.arcadedb.database.RID;
import org.apache.tinkerpop.gremlin.arcadedb.structure.io.ArcadeIoRegistry;
import org.apache.tinkerpop.shaded.jackson.core.JsonGenerator;
import org.apache.tinkerpop.shaded.jackson.databind.JsonSerializer;
import org.apache.tinkerpop.shaded.jackson.databind.SerializerProvider;
import org.apache.tinkerpop.shaded.jackson.databind.jsontype.TypeSerializer;

import java.io.IOException;

/**
 * Created by Enrico Risa on 06/09/2017.
 */
public final class RIDJacksonSerializer extends JsonSerializer<RID> {

  @Override
  public void serialize(final RID value, final JsonGenerator jgen, final SerializerProvider provider) throws IOException {
    jgen.writeStartObject();
    jgen.writeFieldName(ArcadeIoRegistry.BUCKET_ID);
    jgen.writeNumber(value.getBucketId());
    jgen.writeFieldName(ArcadeIoRegistry.BUCKET_POSITION);
    jgen.writeNumber(value.getPosition());
    jgen.writeEndObject();
  }

  @Override
  public void serializeWithType(final RID value, final JsonGenerator jgen, final SerializerProvider serializers, final TypeSerializer typeSer)
      throws IOException {
    typeSer.writeTypePrefixForScalar(value, jgen);
    jgen.writeString(value.toString());
    typeSer.writeTypeSuffixForScalar(value, jgen);
  }

}
