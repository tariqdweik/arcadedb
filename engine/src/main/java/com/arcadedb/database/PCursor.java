package com.arcadedb.database;

import java.util.Iterator;

public interface PCursor<T extends PIdentifiable> extends Iterable<T>, Iterator<T> {
  long size();
}
