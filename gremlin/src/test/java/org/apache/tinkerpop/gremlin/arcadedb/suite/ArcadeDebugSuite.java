/*
 * Copyright (c) 2018 - Arcade Analytics LTD (https://arcadeanalytics.com)
 */

package org.apache.tinkerpop.gremlin.arcadedb.suite;

import org.apache.tinkerpop.gremlin.AbstractGremlinSuite;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalEngine;
import org.apache.tinkerpop.gremlin.structure.PropertyTest;
import org.junit.runners.model.InitializationError;
import org.junit.runners.model.RunnerBuilder;

public class ArcadeDebugSuite extends AbstractGremlinSuite {
  private static final Class<?>[] allTests = new Class<?>[] {
      //      SerializationTest.class,
      //      IoVertexTest.class
      //            GraphTest.class,
      //            TransactionTest.class,
      //            VertexTest.class
      //            TransactionTest.class
      PropertyTest.class };

  public ArcadeDebugSuite(final Class<?> klass, final RunnerBuilder builder) throws InitializationError {
    super(klass, builder, allTests, null, false, TraversalEngine.Type.STANDARD);
  }
}
