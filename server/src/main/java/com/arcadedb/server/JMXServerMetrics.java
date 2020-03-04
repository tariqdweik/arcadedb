/*
 * Copyright (c) - Arcade Data LTD (https://arcadedata.com)
 */

package com.arcadedb.server;

import com.arcadedb.Constants;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import com.codahale.metrics.jmx.JmxReporter;

public class JMXServerMetrics implements ServerMetrics {
  private MetricRegistry metricsRegistry;
  private JmxReporter    jmxReporter;

  public JMXServerMetrics() {
    metricsRegistry = new MetricRegistry();
    jmxReporter = JmxReporter.forRegistry(metricsRegistry).inDomain(Constants.PRODUCT).build();
    jmxReporter.start();
  }

  @Override
  public void stop() {
    jmxReporter.stop();
    metricsRegistry = null;
  }

  @Override
  public MetricTimer timer(final String name) {
    final Timer.Context t = metricsRegistry.timer(name).time();
    return new MetricTimer() {
      @Override
      public void stop() {
        t.stop();
      }
    };
  }

  @Override
  public MetricMeter meter(final String name) {
    final Meter m = metricsRegistry.meter(name);
    return new MetricMeter() {
      @Override
      public void mark() {
        m.mark();
      }
    };
  }
}
