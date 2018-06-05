/*
 * Copyright (c) 2018 - Arcade Analytics LTD (https://arcadeanalytics.com)
 */
package com.arcadedb.server.ha.message;

import com.arcadedb.server.ha.HAServer;

public class HAMessageFactory {
  private final HAServer server;

  public HAMessageFactory(final HAServer server) {
    this.server = server;
  }

  public HARequestMessage getRequestMessage(final byte type) {
    switch (type) {
    case DatabaseListRequest.ID:
      return new DatabaseListRequest();

    case DatabaseStructureRequest.ID:
      return new DatabaseStructureRequest();

    case FileContentRequest.ID:
      return new FileContentRequest();

    case TxRequest.ID:
      return new TxRequest();

    default:
      return null;
    }
  }

  public HAResponseMessage getResponseMessage(final byte type) {
    switch (type) {
    case DatabaseListResponse.ID:
      return new DatabaseListResponse();

    case DatabaseStructureResponse.ID:
      return new DatabaseStructureResponse();

    case FileContentResponse.ID:
      return new FileContentResponse();

    default:
      return null;
    }
  }
}
