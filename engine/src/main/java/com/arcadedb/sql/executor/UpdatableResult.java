package com.arcadedb.sql.executor;

import com.arcadedb.database.Document;
import com.arcadedb.database.ModifiableDocument;

import java.util.Optional;
import java.util.Set;

/**
 * @author Luigi Dell'Aquila (l.dellaquila-(at)-orientdb.com)
 */
public class UpdatableResult extends ResultInternal {
  protected     ResultInternal     previousValue = null;
  private final ModifiableDocument element;

  public UpdatableResult(ModifiableDocument element) {
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
  public Optional<Document> getElement() {
    return Optional.of(element);
  }

  @Override
  public Document toElement() {
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
