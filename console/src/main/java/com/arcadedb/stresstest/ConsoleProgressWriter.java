/*
 * Copyright (c) 2018 - Arcade Analytics LTD (https://arcadeanalytics.com)
 */
package com.arcadedb.stresstest;

import com.arcadedb.Constants;
import com.arcadedb.stresstest.workload.OWorkload;
import com.arcadedb.utility.SoftThread;

/**
 * Takes care of updating the console with a completion percentage while the stress test is working; it takes the data to show from
 * the OStressTestResults class.
 *
 * @author
 */
public class ConsoleProgressWriter extends SoftThread {

  final private OWorkload workload;
  private       String    lastResult = null;

  public ConsoleProgressWriter(final OWorkload workload) {
    super(Constants.PRODUCT + " Console writer");
    this.workload = workload;
  }

  public void printMessage(final String message) {
    System.out.println(message);
  }

  @Override
  protected void execute() {
    final String result = workload.getPartialResult();
    if (lastResult == null || !lastResult.equals(result))
      System.out.print("\r- Workload in progress " + result);
    lastResult = result;
    try {
      Thread.sleep(300);
    } catch (InterruptedException e) {
      softShutdown();
    }
  }

  @Override
  public void sendShutdown() {
    try {
      execute(); // flushes the final result, if we missed it
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
    super.sendShutdown();
  }
}
