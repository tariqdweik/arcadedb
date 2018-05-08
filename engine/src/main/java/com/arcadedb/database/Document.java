package com.arcadedb.database;

import org.json.JSONObject;

import java.util.Map;
import java.util.Set;

public interface Document extends Record {
  byte RECORD_TYPE = 0;

  Object get(String name);

  Set<String> getPropertyNames();

  String getType();

  JSONObject toJSON();

  Map<String,Object> toMap();
}
