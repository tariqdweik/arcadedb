/*
 * Copyright (c) 2018 - Arcade Analytics LTD (https://arcadeanalytics.com)
 */

package com.arcadedb.schema;

public class Property {
  private final DocumentType owner;
  private final String       name;
  private final Class        type;
  private final int          id;

  public Property(final DocumentType owner, final String name, final Class type) {
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
