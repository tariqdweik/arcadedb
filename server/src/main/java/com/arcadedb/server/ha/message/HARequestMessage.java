/*
 * Copyright (c) 2018 - Arcade Analytics LTD (https://arcadeanalytics.com)
 */
package com.arcadedb.server.ha.message;

public interface HARequestMessage extends HAMessage {
  byte getID();
}
