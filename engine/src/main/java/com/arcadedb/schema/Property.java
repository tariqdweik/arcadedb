/*
 * Copyright (c) - Arcade Analytics LTD (https://arcadeanalytics.com)
 */

package com.arcadedb.schema;

import java.util.Objects;

public class Property {
  private final DocumentType owner;
  private final String       name;
  private final Type         type;
  private final int          id;

  public Property(final DocumentType owner, final String name, final Type type) {
    this.owner = owner;
    this.name = name;
    this.type = type;
    this.id = owner.getSchema().getDictionary().getIdByName(name, true);
  }

  public String getName() {
    return name;
  }

  public Type getType() {
    return type;
  }

  public int getId() {
    return id;
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o)
      return true;
    if (o == null || getClass() != o.getClass())
      return false;
    final Property property = (Property) o;
    return id == property.id && Objects.equals(name, property.name) && Objects.equals(type, property.type);
  }

  @Override
  public int hashCode() {
    return id;
  }
}
