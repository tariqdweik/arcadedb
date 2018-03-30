package com.arcadedb.sql.executor;

import com.arcadedb.database.PDocument;
import com.arcadedb.database.PModifiableDocument;

import java.util.Optional;
import java.util.Set;

/**
 * @author Luigi Dell'Aquila (l.dellaquila-(at)-orientdb.com)
 */
public class OUpdatableResult extends OResultInternal {
  protected OResultInternal previousValue = null;
  private final PModifiableDocument element;

  public OUpdatableResult(PModifiableDocument element) {
    this.element = element;
  }

  @Override
  public <T> T getProperty(String name) {
    return (T) element.get(name);
  }

  @Override
  public Set<String> getPropertyNames() {
    return element.getPropertyNames();
  }

  public boolean hasProperty(String propName) {
    if (element != null && element.getPropertyNames().contains(propName)) {
      return true;
    }
    return false;
  }

  @Override
  public boolean isElement() {
    return true;
  }

  @Override
  public Optional<PDocument> getElement() {
    return Optional.of(element);
  }

  @Override
  public PDocument toElement() {
    return element;
  }

  @Override
  public void setProperty(String name, Object value) {
    element.set(name, value);
  }

  public void removeProperty(String name) {
    element.set(name, null);
  }
}
