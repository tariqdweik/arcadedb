/*
 * Copyright (c) - Arcade Analytics LTD (https://arcadeanalytics.com)
 */

package org.apache.tinkerpop.gremlin.arcadedb.structure.io.gryo;

import com.arcadedb.database.Database;
import com.arcadedb.database.RID;
import org.apache.tinkerpop.shaded.kryo.Kryo;
import org.apache.tinkerpop.shaded.kryo.Serializer;
import org.apache.tinkerpop.shaded.kryo.io.Input;
import org.apache.tinkerpop.shaded.kryo.io.Output;

/**
 * Created by Enrico Risa on 06/09/2017.
 */
public class RIDGyroSerializer extends Serializer<RID> {

  @Override
  public RID read(final Kryo kryo, final Input input, final Class<RID> tinkerGraphClass) {
    return new RID((Database) kryo.getContext().get("database"), input.readString());
  }

  @Override
  public void write(final Kryo kryo, final Output output, final RID rid) {
    output.writeString(rid.toString());
  }

}
