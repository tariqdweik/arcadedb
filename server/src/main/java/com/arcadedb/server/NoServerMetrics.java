/*
 * Copyright (c) - Arcade Analytics LTD (https://arcadeanalytics.com)
 */

package com.arcadedb.server;

/**
 * Mock implementation of server metrics.
 */
public class NoServerMetrics implements ServerMetrics {
  public NoServerMetrics() {
  }

  @Override
  public void stop() {
  }

  @Override
  public MetricTimer timer(String s) {
    return new MetricTimer() {
      @Override
      public void stop() {
      }
    };
  }

  @Override
  public MetricMeter meter(String name) {
    return new MetricMeter() {
      @Override
      public void mark() {
      }
    };
  }
}
