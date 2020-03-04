/*
 * Copyright (c) - Arcade Data LTD (https://arcadedata.com)
 */

package com.arcadedb.database;

import java.util.Iterator;

public interface Cursor extends Iterable<Identifiable>, Iterator<Identifiable> {
  long size();
}
