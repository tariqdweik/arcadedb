/*
 * Copyright (c) - Arcade Data LTD (https://arcadedata.com)
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
