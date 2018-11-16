/*
 * Copyright (c) 2018 - Arcade Analytics LTD (https://arcadeanalytics.com)
 */

package com.arcadedb.database;

import java.util.Iterator;

public interface Cursor extends Iterable<Identifiable>, Iterator<Identifiable> {
  long size();
}
