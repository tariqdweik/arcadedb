/*
 * Copyright (c) 2018 - Arcade Analytics LTD (https://arcadeanalytics.com)
 */

package com.arcadedb.console;

import com.arcadedb.utility.FileUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;

public class ConsoleTest {
  private static final String  DB_PATH = "target/database/console";
  private static       Console console;

  @BeforeEach
  public void populate() throws IOException {
    console = new Console(false);
  }

  @AfterEach
  public void drop() {
    console.close();
    FileUtils.deleteRecursively(new File(DB_PATH));
  }

  @Test
  public void testConnect() throws IOException {
    Assertions.assertTrue(console.parse("connect " + DB_PATH + ";info types"));
  }

  @Test
  public void testCreateClass() throws IOException {
    Assertions.assertTrue(console.parse("connect " + DB_PATH));
    Assertions.assertTrue(console.parse("create class Person"));

    final StringBuilder buffer = new StringBuilder();
    console.setOutput(new ConsoleOutput() {
      @Override
      public void onOutput(final String output) {
        buffer.append(output);
      }
    });
    Assertions.assertTrue(console.parse("info types"));
    Assertions.assertTrue(buffer.toString().contains("Person"));
  }

  @Test
  public void testInsertAndSelectRecord() throws IOException {
    Assertions.assertTrue(console.parse("connect " + DB_PATH));
    Assertions.assertTrue(console.parse("create class Person"));
    Assertions.assertTrue(console.parse("insert into Person set name = 'Jay', lastname='Miner'"));

    final StringBuilder buffer = new StringBuilder();
    console.setOutput(new ConsoleOutput() {
      @Override
      public void onOutput(final String output) {
        buffer.append(output);
      }
    });
    Assertions.assertTrue(console.parse("select from Person"));
    Assertions.assertTrue(buffer.toString().contains("Jay"));
  }

  @Test
  public void testInsertAndRollback() throws IOException {
    Assertions.assertTrue(console.parse("connect " + DB_PATH));
    Assertions.assertTrue(console.parse("create class Person"));
    Assertions.assertTrue(console.parse("insert into Person set name = 'Jay', lastname='Miner'"));
    Assertions.assertTrue(console.parse("rollback"));

    final StringBuilder buffer = new StringBuilder();
    console.setOutput(new ConsoleOutput() {
      @Override
      public void onOutput(final String output) {
        buffer.append(output);
      }
    });
    Assertions.assertTrue(console.parse("select from Person"));
    Assertions.assertFalse(buffer.toString().contains("Jay"));
  }

  @Test
  public void testHelp() throws IOException {
    final StringBuilder buffer = new StringBuilder();
    console.setOutput(new ConsoleOutput() {
      @Override
      public void onOutput(final String output) {
        buffer.append(output);
      }
    });
    Assertions.assertTrue(console.parse("?"));
    Assertions.assertTrue(buffer.toString().contains("quit"));
  }
}