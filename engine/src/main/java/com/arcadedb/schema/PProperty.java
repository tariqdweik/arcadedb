/*
 * Copyright (c) 2018 - Arcade Analytics LTD (https://arcadeanalytics.com)
 */

package com.arcadedb.schema;

public class PProperty {
  private final PDocumentType owner;
  private final String        name;
  private final Class         type;
  private final int           id;

  public PProperty(final PDocumentType owner, final String name, final Class type) {
    this.owner = owner;
    this.name = name;
    this.type = type;
    this.id = owner.getSchema().getDictionary().getIdByName(name, true);
  }

  public String getName() {
    return name;
  }

  public Class getType() {
    return type;
  }

  public int getId() {
    return id;
  }
}
