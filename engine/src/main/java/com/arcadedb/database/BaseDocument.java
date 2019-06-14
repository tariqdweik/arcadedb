/*
 * Copyright (c) 2018 - Arcade Analytics LTD (https://arcadeanalytics.com)
 */

package com.arcadedb.database;

import com.arcadedb.schema.Type;

import java.math.BigDecimal;
import java.util.Date;

public abstract class BaseDocument extends BaseRecord implements Document {
  protected final String typeName;
  protected       int    propertiesStartingPosition = 1;

  protected BaseDocument(final Database database, final String typeName, final RID rid, final Binary buffer) {
    super(database, rid, buffer);
    this.typeName = typeName;
  }

  @Override
  public String getString(final String propertyName) {
    return (String) Type.convert(database, get(propertyName), String.class);
  }

  @Override
  public Boolean getBoolean(final String propertyName) {
    return (Boolean) Type.convert(database, get(propertyName), Boolean.class);
  }

  @Override
  public Byte getByte(final String propertyName) {
    return (Byte) Type.convert(database, get(propertyName), Byte.class);
  }

  @Override
  public Short getShort(final String propertyName) {
    return (Short) Type.convert(database, get(propertyName), Short.class);
  }

  @Override
  public Integer getInteger(final String propertyName) {
    return (Integer) Type.convert(database, get(propertyName), Integer.class);
  }

  @Override
  public Long getLong(final String propertyName) {
    return (Long) Type.convert(database, get(propertyName), Long.class);
  }

  @Override
  public Float getFloat(final String propertyName) {
    return (Float) Type.convert(database, get(propertyName), Float.class);
  }

  @Override
  public Double getDouble(final String propertyName) {
    return (Double) Type.convert(database, get(propertyName), Double.class);
  }

  @Override
  public BigDecimal getDecimal(final String propertyName) {
    return (BigDecimal) Type.convert(database, get(propertyName), BigDecimal.class);
  }

  @Override
  public Date getDate(final String propertyName) {
    return (Date) Type.convert(database, get(propertyName), Date.class);
  }

  @Override
  public Document getEmbedded(final String propertyName) {
    return (Document) Type.convert(database, get(propertyName), Document.class);
  }

  public String getType() {
    return typeName;
  }

  @Override
  public byte getRecordType() {
    return RECORD_TYPE;
  }

  @Override
  public void reload() {
    super.reload();
    if (buffer != null)
      buffer.position(propertiesStartingPosition);
  }
}
