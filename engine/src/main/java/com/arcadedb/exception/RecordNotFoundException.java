/*
 * Copyright (c) - Arcade Data LTD (https://arcadedata.com)
 */

package com.arcadedb.exception;

import com.arcadedb.database.RID;

public class RecordNotFoundException extends ArcadeDBException {
  private final RID rid;

  public RecordNotFoundException(final String s, RID rid) {
    super(s);
    this.rid = rid;
  }

  public RecordNotFoundException(String s, RID rid, Exception e) {
    super(s, e);
    this.rid = rid;
  }

  public RID getRID() {
    return rid;
  }
}
