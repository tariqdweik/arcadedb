/*
 * Copyright (c) 2018 - Arcade Analytics LTD (https://arcadeanalytics.com)
 */
package com.arcadedb.stresstest.workload;

import com.arcadedb.stresstest.DatabaseIdentifier;
import com.arcadedb.stresstest.StressTesterSettings;
import org.json.JSONObject;

/**
 * Represents a workload for the stress test.
 *
 * @author Luca Garulli (l.garulli--(at)--orientdb.com)
 */
public interface OWorkload {
  String getName();

  void parseParameters(String params);

  void execute(StressTesterSettings settings, DatabaseIdentifier database);

  String getPartialResult();

  String getFinalResult();

  JSONObject getFinalResultAsJson();
}
