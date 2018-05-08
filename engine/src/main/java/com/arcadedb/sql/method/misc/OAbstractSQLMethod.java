/*
 * Copyright (c) 2018 - Arcade Analytics LTD (https://arcadeanalytics.com)
 */
package com.arcadedb.sql.method.misc;

import com.arcadedb.database.Document;
import com.arcadedb.database.Identifiable;
import com.arcadedb.sql.executor.SQLMethod;

/**
 * 
 * @author Johann Sorel (Geomatys)
 */
public abstract class OAbstractSQLMethod implements SQLMethod {

  private final String name;
  private final int    minparams;
  private final int    maxparams;

  public OAbstractSQLMethod(String name) {
    this(name, 0);
  }

  public OAbstractSQLMethod(String name, int nbparams) {
    this(name, nbparams, nbparams);
  }

  public OAbstractSQLMethod(String name, int minparams, int maxparams) {
    this.name = name;
    this.minparams = minparams;
    this.maxparams = maxparams;
  }

  @Override
  public String getName() {
    return name;
  }

  @Override
  public String getSyntax() {
    final StringBuilder sb = new StringBuilder("<field>.");
    sb.append(getName());
    sb.append('(');
    for (int i = 0; i < minparams; i++) {
      if (i != 0) {
        sb.append(", ");
      }
      sb.append("param");
      sb.append(i + 1);
    }
    if (minparams != maxparams) {
      sb.append('[');
      for (int i = minparams; i < maxparams; i++) {
        if (i != 0) {
          sb.append(", ");
        }
        sb.append("param");
        sb.append(i + 1);
      }
      sb.append(']');
    }
    sb.append(')');

    return sb.toString();
  }

  @Override
  public int getMinParams() {
    return minparams;
  }

  @Override
  public int getMaxParams() {
    return maxparams;
  }

  protected Object getParameterValue(final Identifiable iRecord, final String iValue) {
    if (iValue == null) {
      return null;
    }

    if (iValue.charAt(0) == '\'' || iValue.charAt(0) == '"') {
      // GET THE VALUE AS STRING
      return iValue.substring(1, iValue.length() - 1);
    }

    if(iRecord == null){
      return null;
    }
    // SEARCH FOR FIELD
    return ((Document) iRecord.getRecord()).get(iValue);
  }

  @Override
  public int compareTo(SQLMethod o) {
    return this.getName().compareTo(o.getName());
  }

  @Override
  public String toString() {
    return name;
  }

  @Override
  public boolean evaluateParameters() {
    return true;
  }
}
