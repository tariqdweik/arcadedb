package com.arcadedb.remote;

public class PRemoteException extends RuntimeException {
  public PRemoteException() {
  }

  public PRemoteException(String message) {
    super(message);
  }

  public PRemoteException(String message, Throwable cause) {
    super(message, cause);
  }

  public PRemoteException(Throwable cause) {
    super(cause);
  }

  public PRemoteException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
    super(message, cause, enableSuppression, writableStackTrace);
  }
}
