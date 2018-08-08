/*
 * Copyright (c) 2018 - Arcade Analytics LTD (https://arcadeanalytics.com)
 */

package com.arcadedb.redisw;

import com.arcadedb.GlobalConfiguration;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import redis.clients.jedis.Jedis;

public class RedisWTest extends BaseGraphServerTest {

  private static final int DEF_PORT = 6379;

  @BeforeEach
  @Override
  public void beginTest() {
    GlobalConfiguration.SERVER_PLUGINS.setValue("Redis:com.arcadedb.redisw.RedisWrapperPlugin");
    super.beginTest();
  }

  @AfterEach
  @Override
  public void endTest() {
    GlobalConfiguration.SERVER_PLUGINS.setValue("");
    super.endTest();
  }

  @Test
  public void testSetGet() {
    Jedis jedis = new Jedis("localhost", DEF_PORT);
    for (int i = 0; i < 10000; ++i) {
      jedis.set("foo", "bar");
      Assertions.assertEquals("bar", jedis.get("foo"));
    }
  }
}