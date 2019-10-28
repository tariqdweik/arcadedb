/*
 * Copyright (c) - Arcade Analytics LTD (https://arcadeanalytics.com)
 */

package com.arcadedb.remote;

public class RemoteException extends RuntimeException {
  public RemoteException() {
  }

  public RemoteException(String message) {
    super(message);
  }

  public RemoteException(String message, Throwable cause) {
    super(message, cause);
  }

  public RemoteException(Throwable cause) {
    super(cause);
  }

  public RemoteException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
    super(message, cause, enableSuppression, writableStackTrace);
  }
}
