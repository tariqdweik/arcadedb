/*
 * Copyright (c) 2018 - Arcade Analytics LTD (https://arcadeanalytics.com)
 */

package com.arcadedb;

import com.arcadedb.log.DefaultLogger;
import com.arcadedb.log.LogManager;
import com.arcadedb.log.Logger;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.logging.Level;

public class LoggerTest extends BaseTest {
  private boolean logged  = false;
  private boolean flushed = false;

  @Test
  public void testCustomLogger() {
    try {
      LogManager.instance().setLogger(new Logger() {
        @Override
        public void log(Object iRequester, Level iLevel, String iMessage, Throwable iException, boolean extractDBData, String context,
            Object... iAdditionalArgs) {
          logged = true;
        }

        @Override
        public void flush() {
          flushed = true;
        }
      });

      LogManager.instance().debug(this, "This is a test");

      Assertions.assertEquals(true, logged);

      LogManager.instance().flush();

      Assertions.assertEquals(true, flushed);
    } finally {
      LogManager.instance().setLogger(new DefaultLogger());
    }
  }
}