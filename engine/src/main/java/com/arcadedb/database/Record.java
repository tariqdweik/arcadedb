/*
 * Copyright (c) - Arcade Analytics LTD (https://arcadeanalytics.com)
 */

package com.arcadedb.database;

import org.json.JSONObject;

public interface Record extends Identifiable {
  RID getIdentity();

  byte getRecordType();

  Database getDatabase();

  void reload();

  void delete();

  JSONObject toJSON();
}
