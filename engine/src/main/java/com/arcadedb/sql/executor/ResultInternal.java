/*
 * Copyright (c) 2018 - Arcade Analytics LTD (https://arcadeanalytics.com)
 */

package com.arcadedb.sql.executor;

import com.arcadedb.database.Document;
import com.arcadedb.database.RID;
import com.arcadedb.database.Record;

import java.util.*;
import java.util.stream.Collectors;

/**
 * Created by luigidellaquila on 06/07/16.
 */
public class ResultInternal implements Result {
  protected Map<String, Object> content;
  protected Map<String, Object> metadata;
  protected Document            element;

  public ResultInternal() {
    content = new LinkedHashMap<>();
  }

  public ResultInternal(final Map<String, Object> map) {
    this.content = map;
  }

  public ResultInternal(final Document ident) {
    this.element = ident;
  }

  public void setProperty(String name, Object value) {
    if (value instanceof Optional) {
      value = ((Optional) value).orElse(null);
    }
    if (value instanceof Result && ((Result) value).isElement()) {
      content.put(name, ((Result) value).getElement().get());
    } else {
      content.put(name, value);
    }
  }

  public void removeProperty(String name) {
    content.remove(name);
  }

  public <T> T getProperty(String name) {
    T result = null;
    if (content.containsKey(name)) {
      result = (T) wrap(content.get(name));
    } else if (element != null) {
      result = (T) wrap(element.get(name));
    }
    if (result instanceof Record) {
      result = (T) ((Record) result).getIdentity();
    }
    return result;
  }

  @Override
  public Record getElementProperty(String name) {
    Object result = null;
    if (content.containsKey(name)) {
      result = content.get(name);
    } else if (element != null) {
      result = element.get(name);
    }

    if (result instanceof Result) {
      result = ((Result) result).getRecord().orElse(null);
    }

//    if (result instanceof PRID) {
//      result = ((PRID) result).getRecord();
//    }

    return result instanceof Record ? (Record) result : null;
  }

  private Object wrap(Object input) {
    //TODO!!!
//    if (input instanceof PRecord && !((PRecord) input).getIdentity().isValid()) {
//      OResultInternal result = new OResultInternal();
//      PRecord elem = (PRecord) input;
//      for (String prop : elem.getPropertyNames()) {
//        result.setProperty(prop, elem.get(prop));
//      }
//      //TODO
////      elem.getSchemaType().ifPresent(x -> result.setProperty("@class", x.getName()));
//      return result;
//    } else if (isEmbeddedList(input)) {
//      return ((List) input).stream().map(this::wrap).collect(Collectors.toList());
//    } else if (isEmbeddedSet(input)) {
//      return ((Set) input).stream().map(this::wrap).collect(Collectors.toSet());
//    } else if (isEmbeddedMap(input)) {
//      Map result = new HashMap();
//      for (Map.Entry<Object, Object> o : ((Map<Object, Object>) input).entrySet()) {
//        result.put(o.getKey(), wrap(o.getValue()));
//      }
//      return result;
//    }
    return input;
  }

  private boolean isEmbeddedSet(Object input) {
    if (input instanceof Set) {
      for (Object o : (Set) input) {
        if (o instanceof Record) {
          return false;
        }
        if (isEmbeddedList(o)) {
          return true;
        }
        if (isEmbeddedSet(o)) {
          return true;
        }
        if (isEmbeddedMap(o)) {
          return true;
        }
      }
    }
    return false;
  }

  private boolean isEmbeddedMap(Object input) {
    if (input instanceof Map) {
      for (Object o : ((Map) input).values()) {
        if (o instanceof Record) {
          return false;//TODO
        }
        if (isEmbeddedList(o)) {
          return true;
        }
        if (isEmbeddedSet(o)) {
          return true;
        }
        if (isEmbeddedMap(o)) {
          return true;
        }
      }
    }
    return false;
  }

  private boolean isEmbeddedList(Object input) {
    if (input instanceof List) {
      for (Object o : (List) input) {
        if (o instanceof Record) {
          return false;
        }
        if (isEmbeddedList(o)) {
          return true;
        }
        if (isEmbeddedSet(o)) {
          return true;
        }
        if (isEmbeddedMap(o)) {
          return true;
        }
      }
    }
    return false;
  }

  public Set<String> getPropertyNames() {
    Set<String> result = new LinkedHashSet<>();
    if (element != null) {
      result.addAll(element.getPropertyNames());
    }
    result.addAll(content.keySet());
    return result;
  }

  public boolean hasProperty(String propName) {
    if (element != null && element.getPropertyNames().contains(propName)) {
      return true;
    }
    return content.keySet().contains(propName);
  }

  @Override
  public boolean isElement() {
    return this.element != null;
  }

  public Optional<Document> getElement() {
    return Optional.ofNullable(element);
  }

  @Override
  public Document toElement() {
    if (isElement()) {
      return getElement().get();
    }
    throw new UnsupportedOperationException("REVIEW THIS!!!");
  }

  @Override
  public Optional<RID> getIdentity() {
    if (element != null) {
      return Optional.of(element.getIdentity());
    }
    return Optional.empty();
  }

  @Override
  public boolean isProjection() {
    return this.element == null;
  }

  @Override
  public Optional<Record> getRecord() {
    return Optional.ofNullable(this.element);
  }

  @Override
  public Object getMetadata(String key) {
    if (key == null) {
      return null;
    }
    return metadata == null ? null : metadata.get(key);
  }

  public void setMetadata(String key, Object value) {
    if (key == null) {
      return;
    }
    if (metadata == null) {
      metadata = new HashMap<>();
    }
    metadata.put(key, value);
  }

  public void clearMetadata() {
    metadata = null;
  }

  public void removeMetadata(String key) {
    if (key == null || metadata == null) {
      return;
    }
    metadata.remove(key);
  }

  public void addMetadata(Map<String, Object> values) {
    if (values == null) {
      return;
    }
    if (this.metadata == null) {
      this.metadata = new HashMap<>();
    }
    this.metadata.putAll(values);
  }

  @Override
  public Set<String> getMetadataKeys() {
    return metadata == null ? Collections.emptySet() : metadata.keySet();
  }

  private Object convertToElement(Object property) {
    if (property instanceof Result) {
      return ((Result) property).toElement();
    }
    if (property instanceof List) {
      return ((List) property).stream().map(x -> convertToElement(x)).collect(Collectors.toList());
    }

    if (property instanceof Set) {
      return ((Set) property).stream().map(x -> convertToElement(x)).collect(Collectors.toSet());
    }

    if (property instanceof Map) {
      Map<Object, Object> result = new HashMap<>();
      Map<Object, Object> prop = ((Map) property);
      for (Map.Entry<Object, Object> o : prop.entrySet()) {
        result.put(o.getKey(), convertToElement(o.getValue()));
      }
    }

    return property;
  }

  public void setElement(Document element) {
    this.element = element;
  }

  @Override
  public String toString() {
    if (element != null) {
      return element.toString();
    }
    return "{\n" + content.entrySet().stream().map(x -> x.getKey() + ": " + x.getValue()).reduce("", (a, b) -> a + b + "\n")
        + "}\n";
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (!(obj instanceof ResultInternal)) {
      return false;
    }
    ResultInternal resultObj = (ResultInternal) obj;
    if (element != null) {
      if (!resultObj.getElement().isPresent()) {
        return false;
      }
      return element.equals(resultObj.getElement().get());
    } else {
      if (resultObj.getElement().isPresent()) {
        return false;
      }
      return this.content.equals(resultObj.content);
    }
  }

  @Override
  public int hashCode() {
    if (element != null) {
      return element.hashCode();
    }
    return content.hashCode();
  }

}
