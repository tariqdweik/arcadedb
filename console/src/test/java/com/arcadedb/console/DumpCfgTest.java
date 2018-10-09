/*
 * Copyright (c) 2018 - Arcade Analytics LTD (https://arcadeanalytics.com)
 */

package com.arcadedb.console;

import com.arcadedb.GlobalConfiguration;

public class DumpCfgTest {
  public static void main(String[] args) {
    System.out.printf("\n|Name|Description|Type|Default Value");
    for (GlobalConfiguration c : GlobalConfiguration.values()) {
      System.out.printf("\n|%s|%s|%s|%s", c.getKey().substring("arcadedb".length() + 1), c.getDescription(), c.getType().getSimpleName(),
          c.getDefValue());
    }


//    System.out.printf("\n|Name|Java API ENUM name|Description|Type|Default Value");
//    for (GlobalConfiguration c : GlobalConfiguration.values()) {
//      System.out.printf("\n|%s|%s|%s|%s|%s", c.getKey().substring("arcadedb".length() + 1), c.name(), c.getDescription(), c.getType().getSimpleName(),
//          c.getDefValue());
//    }
  }
}