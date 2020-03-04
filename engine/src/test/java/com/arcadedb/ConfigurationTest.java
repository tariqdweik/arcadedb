/*
 * Copyright (c) - Arcade Data LTD (https://arcadedata.com)
 */

package com.arcadedb;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import static com.arcadedb.GlobalConfiguration.TEST;

public class ConfigurationTest {
  @Test
  public void testGlobalExport2Json() {
    Assertions.assertFalse(TEST.getValueAsBoolean());

    final String json = GlobalConfiguration.toJSON();

    TEST.setValue(true);

    Assertions.assertTrue(TEST.getValueAsBoolean());

    GlobalConfiguration.fromJSON(json);

    Assertions.assertFalse(TEST.getValueAsBoolean());
  }

  @Test
  public void testContextExport2Json() {
    final ContextConfiguration cfg = new ContextConfiguration();

    cfg.setValue(TEST, false);

    Assertions.assertFalse(cfg.getValueAsBoolean(TEST));

    final String json = cfg.toJSON();

    cfg.setValue(TEST, true);

    Assertions.assertTrue(cfg.getValueAsBoolean(TEST));

    cfg.fromJSON(json);

    Assertions.assertFalse(cfg.getValueAsBoolean(TEST));
  }
}