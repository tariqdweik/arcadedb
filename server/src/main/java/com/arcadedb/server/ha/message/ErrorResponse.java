/*
 * Copyright (c) 2018 - Arcade Analytics LTD (https://arcadeanalytics.com)
 */
package com.arcadedb.server.ha.message;

import com.arcadedb.database.Binary;
import com.arcadedb.server.ha.HAServer;
import com.arcadedb.utility.LogManager;

/**
 * Response for forwarded transaction.
 */
public class ErrorResponse extends HAAbstractCommand {
  public String exceptionClass;
  public String exceptionMessage;

  public ErrorResponse() {
  }

  public ErrorResponse(final Exception exception) {
    this.exceptionClass = exception.getClass().getName();
    this.exceptionMessage = exception.getMessage();
  }

  @Override
  public HACommand execute(final HAServer server, final String remoteServerName, final long messageNumber) {
    LogManager.instance().warn(this, "Received error from remote server (class=%d message=%s)", exceptionClass, exceptionMessage);
    server.receivedResponseFromForward(messageNumber, this);
    return null;
  }

  @Override
  public void toStream(final Binary stream) {
    stream.putString(exceptionClass);
    stream.putString(exceptionMessage);
  }

  @Override
  public void fromStream(final Binary stream) {
    exceptionClass = stream.getString();
    exceptionMessage = stream.getString();
  }

  @Override
  public String toString() {
    return "error-response(" + exceptionClass + ")";
  }
}
