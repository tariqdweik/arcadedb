package com.arcadedb.database;

import java.util.Iterator;

public interface Cursor<T extends Identifiable> extends Iterable<T>, Iterator<T> {
  long size();
}
