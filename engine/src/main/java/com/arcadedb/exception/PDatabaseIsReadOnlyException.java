package com.arcadedb.exception;

import java.io.IOException;

public class PDatabaseIsReadOnlyException extends RuntimeException {
  public PDatabaseIsReadOnlyException(final String s) {
    super(s);
  }

  public PDatabaseIsReadOnlyException(String s, IOException e) {
    super(s, e);
  }
}
