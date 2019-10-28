/*
 * Copyright (c) - Arcade Analytics LTD (https://arcadeanalytics.com)
 */
package com.arcadedb.utility;

/**
 * Generic callable interface that accepts a parameter.
 *
 * @author Luca Garulli
 */
public interface Callable<RET, PAR> {
  RET call(PAR iArgument);
}
