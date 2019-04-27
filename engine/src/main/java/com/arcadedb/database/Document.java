/*
 * Copyright (c) 2018 - Arcade Analytics LTD (https://arcadeanalytics.com)
 */

package com.arcadedb.database;

import org.json.JSONObject;

import java.math.BigDecimal;
import java.util.Date;
import java.util.Map;
import java.util.Set;

public interface Document extends Record {
  byte RECORD_TYPE = 0;

  MutableDocument modify();

  Object get(String propertyName);

  String getString(String propertyName);

  Boolean getBoolean(String propertyName);

  Byte getByte(String propertyName);

  Short getShort(String propertyName);

  Integer getInteger(String propertyName);

  Long getLong(String propertyName);

  Float getFloat(String propertyName);

  Double getDouble(String propertyName);

  BigDecimal getDecimal(String propertyName);

  Date getDate(String propertyName);

  Set<String> getPropertyNames();

  String getType();

  JSONObject toJSON();

  Map<String, Object> toMap();
}
