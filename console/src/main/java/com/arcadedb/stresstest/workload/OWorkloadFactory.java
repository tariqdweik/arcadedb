/*
 * Copyright (c) 2018 - Arcade Analytics LTD (https://arcadeanalytics.com)
 */
package com.arcadedb.stresstest.workload;

import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

/**
 * Factory of workloads.
 *
 * @author Luca Garulli (l.garulli--(at)--orientdb.com)
 */
public class OWorkloadFactory {
  private Map<String, OWorkload> registered = new HashMap<String, OWorkload>();

  public OWorkloadFactory() {
    register(new OCRUDWorkload());
  }

  public OWorkload get(final String name) {
    return registered.get(name.toUpperCase(Locale.ENGLISH));
  }

  public void register(final OWorkload workload) {
    registered.put(workload.getName().toUpperCase(Locale.ENGLISH), workload);
  }

  public Set<String> getRegistered() {
    return registered.keySet();
  }
}
