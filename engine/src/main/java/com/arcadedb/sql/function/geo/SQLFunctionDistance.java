/*
 * Copyright (c) - Arcade Data LTD (https://arcadedata.com)
 */
package com.arcadedb.sql.function.geo;

import com.arcadedb.database.Identifiable;
import com.arcadedb.schema.Type;
import com.arcadedb.sql.executor.CommandContext;
import com.arcadedb.sql.function.SQLFunctionAbstract;

/**
 * Haversine formula to compute the distance between 2 gro points.
 *
 * @author Luca Garulli (l.garulli--(at)--orientdb.com)
 */
public class SQLFunctionDistance extends SQLFunctionAbstract {
  public static final String NAME = "distance";

  private final static double EARTH_RADIUS = 6371;

  public SQLFunctionDistance() {
    super(NAME, 4, 5);
  }

  public Object execute( final Object iThis, final Identifiable iCurrentRecord, Object iCurrentResult,
      final Object[] iParams, CommandContext iContext) {
    double distance;

    final double[] values = new double[4];

    for (int i = 0; i < iParams.length && i < 4; ++i) {
      if (iParams[i] == null)
        return null;

      values[i] = ((Double) Type.convert(iContext.getDatabase(), iParams[i], Double.class)).doubleValue();
    }

    final double deltaLat = Math.toRadians(values[2] - values[0]);
    final double deltaLon = Math.toRadians(values[3] - values[1]);

    final double a =
        Math.pow(Math.sin(deltaLat / 2), 2) + Math.cos(Math.toRadians(values[0])) * Math.cos(Math.toRadians(values[2])) * Math
            .pow(Math.sin(deltaLon / 2), 2);
    distance = 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a)) * EARTH_RADIUS;

    if (iParams.length > 4) {
      final String unit = iParams[4].toString();
      if (unit.equalsIgnoreCase("km"))
        // ALREADY IN KM
        ;
      else if (unit.equalsIgnoreCase("mi"))
        // MILES
        distance *= 0.621371192;
      else if (unit.equalsIgnoreCase("nmi"))
        // NAUTICAL MILES
        distance *= 0.539956803;
      else
        throw new IllegalArgumentException("Unsupported unit '" + unit + "'. Use km, mi and nmi. Default is km.");
    }

    return distance;
  }

  public String getSyntax() {
    return "distance(<field-x>,<field-y>,<x-value>,<y-value>[,<unit>])";
  }
}
