/*
 * Copyright (c) - Arcade Data LTD (https://arcadedata.com)
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
