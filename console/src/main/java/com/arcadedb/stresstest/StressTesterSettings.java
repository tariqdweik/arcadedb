/*
 * Copyright (c) 2018 - Arcade Analytics LTD (https://arcadeanalytics.com)
 */
package com.arcadedb.stresstest;

import com.arcadedb.remote.RemoteDatabase;

/**
 * StressTester settings.
 *
 * @author Luca Garulli (l.garulli--(at)--orientdb.com)
 */
public class StressTesterSettings {
  public String             dbName;
  public StressTester.OMode mode;
  public String             rootPassword;
  public String             resultOutputFile;
  public String             embeddedPath;
  public int                operationsPerTransaction;
  public int                delay;
  public int                concurrencyLevel;
  public String             remoteIp;
  public boolean            haMetrics;
  public String             workloadCfg;
  public boolean            keepDatabaseAfterTest;
  public int                remotePort    = 2424;
  public boolean             checkDatabase = false;
  public RemoteDatabase.CONNECTION_STRATEGY loadBalancing = RemoteDatabase.CONNECTION_STRATEGY.ROUND_ROBIN_REQUEST;
}
