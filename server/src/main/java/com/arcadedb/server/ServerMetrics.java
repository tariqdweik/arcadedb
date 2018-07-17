/*
 * Copyright (c) 2018 - Arcade Analytics LTD (https://arcadeanalytics.com)
 */

package com.arcadedb.server;

public interface ServerMetrics {
  interface MetricMeter {
    void mark();
  }

  interface MetricTimer {
    void stop();
  }

  void stop();

  MetricTimer timer(String name);

  MetricMeter meter(String name);
}
