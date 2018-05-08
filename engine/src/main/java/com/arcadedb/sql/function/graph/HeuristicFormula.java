/*
 * Copyright (c) 2018 - Arcade Analytics LTD (https://arcadeanalytics.com)
 */
package com.arcadedb.sql.function.graph;

/**
 *  Heuristic formula enum.
 *
 * @author Saeed Tabrizi (saeed a_t  nowcando.com)
 */
public enum HeuristicFormula {
    MANHATAN,
    MAXAXIS,
    DIAGONAL,
    EUCLIDEAN,
    EUCLIDEANNOSQR
}