/*
 * Copyright (c) - Arcade Analytics LTD (https://arcadeanalytics.com)
 */

package com.arcadedb.sql.parser;

import com.arcadedb.database.Identifiable;
import com.arcadedb.sql.executor.CommandContext;

import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 * Created by luigidellaquila on 02/10/15.
 */
public class OQueryCursor implements Iterator<Identifiable> {
  private int                    limit;
  private int                    skip;
  private WhereClause            filter;
  private Iterator<Identifiable> iterator;
  private OrderBy                orderBy;
  private CommandContext         ctx;

  private Identifiable next         = null;
  private long         countFetched = 0;

  public OQueryCursor() {

  }

  public OQueryCursor(Iterator<Identifiable> PIdentifiableIterator, WhereClause filter, OrderBy orderBy, int skip, int limit,
      CommandContext ctx) {
    this.iterator = PIdentifiableIterator;
    this.filter = filter;
    this.skip = skip;
    this.limit = limit;
    this.orderBy = orderBy;
    this.ctx = ctx;
    loadNext();
  }

  private void loadNext() {
    if (iterator == null) {
      next = null;
      return;
    }
    if (limit > 0 && countFetched >= limit) {
      next = null;
      return;
    }
    if (countFetched == 0 && skip > 0) {
      for (int i = 0; i < skip; i++) {
        next = getNextFromIterator();
        if (next == null) {
          return;
        }
      }
    }
    next = getNextFromIterator();
    countFetched++;
  }

  private Identifiable getNextFromIterator() {
    while (true) {
      if (iterator == null || !iterator.hasNext()) {
        return null;
      }

      Identifiable result = iterator.next();
      if (filter==null || filter.matchesFilters(result, ctx)) {
        return result;
      }
    }
  }

  public boolean hasNext() {
    return next != null;
  }

  public void remove() {
    throw new UnsupportedOperationException("remove");
  }

  public Identifiable next() {
    Identifiable result = next;
    if (result == null) {
      throw new NoSuchElementException();
    }
    loadNext();
    return result;
  }
}
