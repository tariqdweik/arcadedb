/*
 * Copyright (c) - Arcade Data LTD (https://arcadedata.com)
 */

package com.arcadedb.redisw;

import com.arcadedb.GlobalConfiguration;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import redis.clients.jedis.Jedis;

public class RedisWTest extends BaseGraphServerTest {

  private static final int DEF_PORT = 6379;
  private static final int TOTAL    = 10_000;

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
  public void testSet() {
    Jedis jedis = new Jedis("localhost", DEF_PORT);

    long beginTime = System.currentTimeMillis();

    for (int i = 0; i < TOTAL; ++i) {
      jedis.set("foo" + i, String.valueOf(i));
    }

    System.out.println("Inserted  " + TOTAL + " items. Elapsed" + (System.currentTimeMillis() - beginTime) + "ms");

    beginTime = System.currentTimeMillis();

    for (int i = 0; i < TOTAL; ++i) {
      jedis.get("foo" + i);
      //Assertions.assertEquals(String.valueOf(i), jedis.get("foo" + i));
    }

    System.out.println("Retrieved  " + TOTAL + " items. Elapsed" + (System.currentTimeMillis() - beginTime) + "ms");

  }

}