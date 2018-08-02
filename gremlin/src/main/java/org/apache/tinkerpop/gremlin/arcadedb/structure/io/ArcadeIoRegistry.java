/*
 * Copyright (c) 2018 - Arcade Analytics LTD (https://arcadeanalytics.com)
 */

package org.apache.tinkerpop.gremlin.arcadedb.structure.io;

import com.arcadedb.database.Database;
import com.arcadedb.database.RID;
import org.apache.tinkerpop.gremlin.arcadedb.structure.io.graphson.ArcadeGraphSONV3;
import org.apache.tinkerpop.gremlin.arcadedb.structure.io.gryo.RIDGyroSerializer;
import org.apache.tinkerpop.gremlin.structure.io.AbstractIoRegistry;
import org.apache.tinkerpop.gremlin.structure.io.graphson.GraphSONIo;
import org.apache.tinkerpop.gremlin.structure.io.gryo.GryoIo;

import java.util.Map;

@SuppressWarnings("serial")
public class ArcadeIoRegistry extends AbstractIoRegistry {

  public static final String BUCKET_ID       = "bucketId";
  public static final String BUCKET_POSITION = "bucketPosition";

  private static final ArcadeIoRegistry INSTANCE = new ArcadeIoRegistry();

  private ArcadeIoRegistry() {
    register(GryoIo.class, RID.class, new RIDGyroSerializer());
    register(GraphSONIo.class, RID.class, ArcadeGraphSONV3.INSTANCE);
  }

  public static ArcadeIoRegistry instance() {
    return INSTANCE;
  }

  public static ArcadeIoRegistry getInstance() {
    return INSTANCE;
  }

  public static RID newRID(final Database db, final Object obj) {
    if (obj == null)
      return null;

    if (obj instanceof RID)
      return (RID) obj;

    if (obj instanceof Map) {
      @SuppressWarnings({ "unchecked", "rawtypes" })
      final Map<String, Number> map = (Map) obj;
      return new RID(db, map.get(BUCKET_ID).intValue(), map.get(BUCKET_POSITION).longValue());
    }

    throw new IllegalArgumentException("Unable to convert unknown (" + obj.getClass() + ") type to RID");
  }

  public static boolean isRID(final Object result) {
    if (!(result instanceof Map))
      return false;

    @SuppressWarnings("unchecked")
    final Map<String, Number> map = (Map<String, Number>) result;
    return map.containsKey(BUCKET_ID) && map.containsKey(BUCKET_POSITION);
  }

}
