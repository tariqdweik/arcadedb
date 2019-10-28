/*
 * Copyright (c) - Arcade Analytics LTD (https://arcadeanalytics.com)
 */
package com.arcadedb.sql.function;

import com.arcadedb.sql.function.coll.*;
import com.arcadedb.sql.function.geo.SQLFunctionDistance;
import com.arcadedb.sql.function.graph.*;
import com.arcadedb.sql.function.math.*;
import com.arcadedb.sql.function.misc.*;
import com.arcadedb.sql.function.stat.*;
import com.arcadedb.sql.function.text.SQLFunctionConcat;
import com.arcadedb.sql.function.text.SQLFunctionFormat;

/**
 * Default set of SQL function.
 *
 * @author Johann Sorel (Geomatys)
 */
public final class DefaultSQLFunctionFactory extends SQLFunctionFactoryTemplate {
  public DefaultSQLFunctionFactory() {
    // MISC FUNCTIONS
    register(SQLFunctionAverage.NAME, SQLFunctionAverage.class);
    register(SQLFunctionCoalesce.NAME, new SQLFunctionCoalesce());
    register(SQLFunctionCount.NAME, SQLFunctionCount.class);
    register(SQLFunctionDate.NAME, SQLFunctionDate.class);
    register(SQLFunctionDecode.NAME, new SQLFunctionDecode());
    register(SQLFunctionDifference.NAME, SQLFunctionDifference.class);
    register(SQLFunctionSymmetricDifference.NAME, SQLFunctionSymmetricDifference.class);
    register(SQLFunctionDistance.NAME, new SQLFunctionDistance());
    register(SQLFunctionDistinct.NAME, SQLFunctionDistinct.class);
    register(SQLFunctionEncode.NAME, new SQLFunctionEncode());
    register(SQLFunctionFirst.NAME, new SQLFunctionFirst());
    register(SQLFunctionFormat.NAME, new SQLFunctionFormat());
    register(SQLFunctionIf.NAME, new SQLFunctionIf());
    register(SQLFunctionIfNull.NAME, new SQLFunctionIfNull());
    register(SQLFunctionIntersect.NAME, SQLFunctionIntersect.class);
    register(SQLFunctionLast.NAME, new SQLFunctionLast());
    register(SQLFunctionList.NAME, SQLFunctionList.class);
    register(SQLFunctionMap.NAME, SQLFunctionMap.class);
    register(SQLFunctionMax.NAME, SQLFunctionMax.class);
    register(SQLFunctionMin.NAME, SQLFunctionMin.class);
    register(SQLFunctionSet.NAME, SQLFunctionSet.class);
    register(SQLFunctionSysdate.NAME, SQLFunctionSysdate.class);
    register(SQLFunctionSum.NAME, SQLFunctionSum.class);
    register(SQLFunctionUnionAll.NAME, SQLFunctionUnionAll.class);
    register(SQLFunctionMode.NAME, SQLFunctionMode.class);
    register(SQLFunctionPercentile.NAME, SQLFunctionPercentile.class);
    register(SQLFunctionMedian.NAME, SQLFunctionMedian.class);
    register(SQLFunctionVariance.NAME, SQLFunctionVariance.class);
    register(SQLFunctionStandardDeviation.NAME, SQLFunctionStandardDeviation.class);
    register(SQLFunctionUUID.NAME, SQLFunctionUUID.class);
    register(SQLFunctionConcat.NAME, SQLFunctionConcat.class);
    register(SQLFunctionAbsoluteValue.NAME, SQLFunctionAbsoluteValue.class);
    //graph
    register(SQLFunctionOut.NAME, SQLFunctionOut.class);
    register(SQLFunctionIn.NAME, SQLFunctionIn.class);
    register(SQLFunctionBoth.NAME, SQLFunctionBoth.class);
    register(SQLFunctionOutE.NAME, SQLFunctionOutE.class);
    register(SQLFunctionOutV.NAME, SQLFunctionOutV.class);
    register(SQLFunctionInE.NAME, SQLFunctionInE.class);
    register(SQLFunctionInV.NAME, SQLFunctionInV.class);
    register(SQLFunctionBothE.NAME, SQLFunctionBothE.class);
    register(SQLFunctionBothV.NAME, SQLFunctionBothV.class);
    register(SQLFunctionShortestPath.NAME, SQLFunctionShortestPath.class);
    register(SQLFunctionDijkstra.NAME, SQLFunctionDijkstra.class);
    register(SQLFunctionAstar.NAME, SQLFunctionAstar.class);
  }
}
