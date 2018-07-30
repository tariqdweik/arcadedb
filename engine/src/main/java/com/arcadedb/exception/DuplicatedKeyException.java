/*
 * Copyright (c) 2018 - Arcade Analytics LTD (https://arcadeanalytics.com)
 */

package com.arcadedb.exception;

import java.util.Arrays;

public class DuplicatedKeyException extends RuntimeException {
  public DuplicatedKeyException(final String indexName, final Object[] keys) {
    super("Duplicated key " + Arrays.toString(keys) + " found on index '" + indexName + "'");
  }
}
