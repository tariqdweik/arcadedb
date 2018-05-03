package com.arcadedb.exception;

import com.arcadedb.database.PRID;

import java.io.IOException;

public class PRecordNotFoundException extends RuntimeException {
  private final PRID rid;

  public PRecordNotFoundException(final String s, PRID rid) {
    super(s);
    this.rid = rid;
  }

  public PRecordNotFoundException(String s, PRID rid, IOException e) {
    super(s, e);
    this.rid = rid;
  }

  public PRID getRID() {
    return rid;
  }
}
