/*
 * Copyright 2021 Arcade Data Ltd
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package performance;

import com.arcadedb.BaseTest;
import com.arcadedb.database.async.ErrorCallback;
import com.arcadedb.engine.WALFile;
import com.arcadedb.graph.Edge;
import com.arcadedb.graph.MutableVertex;
import com.arcadedb.graph.Vertex;
import com.arcadedb.index.IndexCursor;
import com.arcadedb.log.LogManager;
import com.arcadedb.log.Logger;
import com.arcadedb.schema.SchemaImpl;
import com.arcadedb.schema.VertexType;
import com.arcadedb.utility.FileUtils;
import org.junit.jupiter.api.Assertions;

import java.io.File;
import java.util.logging.Level;

/**
 * Inserts a graph. Configurations:
 * - 100M edges on 10,000 vertices with respectively 10,000 to all the other nodes and itself. 10,000 * 10,000 = 100M edges
 * - 1B edges on 31,623 vertices with respectively 31,623 edges to all the other nodes and itself. 31,623 * 31,623 = 1B edges
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class PerformanceInsertGraphIndexTest extends BaseTest {
  private static final int     VERTICES         = 10_000; //31_623;
  private static final int     EDGES_PER_VERTEX = 10_000; //31_623;
  private static final String  VERTEX_TYPE_NAME = "Person";
  private static final String  EDGE_TYPE_NAME   = "Friend";
  private static final int     PARALLEL         = 6;
  private static final boolean USE_WAL          = true;

  public static void main(String[] args) {
    final long begin = System.currentTimeMillis();

    FileUtils.deleteRecursively(new File(PerformanceTest.DATABASE_PATH));

    final PerformanceInsertGraphIndexTest test = new PerformanceInsertGraphIndexTest();

    // PHASE 1
    {
      test.createSchema();
      test.createVertices();
      test.loadVertices();
      test.createEdges();
    }

    // PHASE 2
    {
      Vertex[] cachedVertices = test.loadVertices();
      test.checkGraph(cachedVertices);
    }

    test.database.close();

    final long elapsedSecs = (System.currentTimeMillis() - begin) / 1000;
    System.out.println("TEST completed in " + elapsedSecs + " secs = " + (elapsedSecs / 60) + " mins");
  }

  protected PerformanceInsertGraphIndexTest() {
    super(false);
  }

  @Override
  protected String getDatabasePath() {
    return PerformanceTest.DATABASE_PATH;
  }

  @Override
  protected String getPerformanceProfile() {
    LogManager.instance().setLogger(new Logger() {
      @Override
      public void log(Object iRequester, Level iLevel, String iMessage, Throwable iException, String context, Object arg1, Object arg2, Object arg3,
          Object arg4, Object arg5, Object arg6, Object arg7, Object arg8, Object arg9, Object arg10, Object arg11, Object arg12, Object arg13, Object arg14,
          Object arg15, Object arg16, Object arg17) {
      }

      @Override
      public void log(Object iRequester, Level iLevel, String iMessage, Throwable iException, String context, Object... args) {
      }

      @Override
      public void flush() {
      }
    });

    return "high-performance";
  }

  private void createEdges() {
    System.out.println("Creating " + EDGES_PER_VERTEX + " edges per vertex on all " + VERTICES + " vertices");

    database.async().close();

    database.begin();

    if (!USE_WAL) {
      database.getTransaction().setUseWAL(false);
      database.getTransaction().setWALFlush(WALFile.FLUSH_TYPE.NO);
    }

    final long begin = System.currentTimeMillis();
    try {
      int sourceIndex = 0;
      for (; sourceIndex < VERTICES; ++sourceIndex) {
        int edges = 0;

        final Vertex sourceVertex = (Vertex) database.lookupByKey(VERTEX_TYPE_NAME, new String[] { "id" }, new Object[] { sourceIndex }).next().getRecord();

        for (int destinationIndex = 0; destinationIndex < VERTICES; destinationIndex++) {
          final Vertex destinationVertex = (Vertex) database.lookupByKey(VERTEX_TYPE_NAME, new String[] { "id" }, new Object[] { destinationIndex }).next()
              .getRecord();

          sourceVertex.newEdge(EDGE_TYPE_NAME, destinationVertex, true);
          if (++edges > EDGES_PER_VERTEX)
            break;
        }

        if (sourceIndex % 100 == 0)
          System.out.println("Created " + edges + " edges per vertex in " + sourceIndex + " vertices in " + (System.currentTimeMillis() - begin) + "ms");

        if (sourceIndex % 20 == 0) {
          database.commit();
          database.begin();
          if (!USE_WAL) {
            database.getTransaction().setUseWAL(false);
            database.getTransaction().setWALFlush(WALFile.FLUSH_TYPE.NO);
          }
        }
      }
      System.out.println("Created " + EDGES_PER_VERTEX + " edges per vertex in " + sourceIndex + " vertices in " + (System.currentTimeMillis() - begin) + "ms");

    } finally {
      database.commit();
      final long elapsed = System.currentTimeMillis() - begin;
      System.out.println("Creation of edges finished in " + elapsed + "ms");
    }
  }

  private Vertex[] loadVertices() {
    final Vertex[] cachedVertices = new Vertex[VERTICES];

    System.out.println("Loading " + VERTICES + " vertices in RAM...");
    database.transaction((tx) -> {
      final long begin = System.currentTimeMillis();
      try {
        int counter = 0;
        for (; counter < VERTICES; ++counter) {
          final IndexCursor cursor = database.lookupByKey(VERTEX_TYPE_NAME, new String[] { "id" }, new Object[] { counter });
          if (!cursor.hasNext()) {
            System.out.println("Vertex with id " + counter + " was not found");
            continue;
          }

          cachedVertices[counter] = (Vertex) cursor.next().getRecord();
        }
        System.out.println("Loaded " + counter + " vertices in " + (System.currentTimeMillis() - begin) + "ms");
      } finally {
        final long elapsed = System.currentTimeMillis() - begin;
        System.out.println("Loaded all vertices in RAM in " + elapsed + "ms -> " + (VERTICES / (elapsed / 1000F)) + " ops/sec");
      }
    });
    return cachedVertices;
  }

  private void createVertices() {
    System.out.println("Start inserting " + VERTICES + " vertices...");

    long startOfTest = System.currentTimeMillis();

    try {
      //database.setEdgeListSize(256);

      database.async().setParallelLevel(PARALLEL);
      database.async().setTransactionUseWAL(false);
      database.async().setTransactionSync(WALFile.FLUSH_TYPE.NO);
      database.async().setCommitEvery(5000);
      database.async().onError(new ErrorCallback() {
        @Override
        public void call(Exception exception) {
          LogManager.instance().log(this, Level.SEVERE, "ERROR: " + exception, exception);
          System.exit(1);
        }
      });

      long counter = 0;
      for (; counter < VERTICES; ++counter) {
        final MutableVertex vertex = database.newVertex(VERTEX_TYPE_NAME);

        vertex.set("id", counter);

        database.async().createRecord(vertex, null);

        if (counter % 1_000_000 == 0)
          System.out.println("Inserted " + counter + " vertices in " + (System.currentTimeMillis() - startOfTest) + "ms");
      }

      System.out.println("Inserted " + counter + " vertices in " + (System.currentTimeMillis() - startOfTest) + "ms");

    } finally {
      database.async().waitCompletion();
      final long elapsed = System.currentTimeMillis() - startOfTest;
      System.out.println("Insertion finished in " + elapsed + "ms -> " + (VERTICES / (elapsed / 1000F)) + " ops/sec");
    }
  }

  private void createSchema() {
    database.begin();

    final VertexType type = database.getSchema().createVertexType(VERTEX_TYPE_NAME, PARALLEL);
    type.createProperty("id", Long.class);

    database.getSchema().createEdgeType(EDGE_TYPE_NAME, PARALLEL);

    database.getSchema().createTypeIndex(SchemaImpl.INDEX_TYPE.LSM_TREE, false, VERTEX_TYPE_NAME, new String[] { "id" }, 5000000);

    database.commit();
  }

  private void checkGraph(Vertex[] cachedVertices) {
    System.out.println("Checking graph with " + VERTICES + " vertices");

    database.begin();
    final long begin = System.currentTimeMillis();

    final int expectedEdges = Math.min(VERTICES, EDGES_PER_VERTEX);

    try {
      int i = 0;
      for (; i < VERTICES; ++i) {
        int outEdges = 0;
        for (Edge e : cachedVertices[i].getEdges(Vertex.DIRECTION.OUT, EDGE_TYPE_NAME))
          ++outEdges;
        Assertions.assertEquals(expectedEdges, outEdges);

        int inEdges = 0;
        for (Edge e : cachedVertices[i].getEdges(Vertex.DIRECTION.IN, EDGE_TYPE_NAME))
          ++inEdges;
        Assertions.assertEquals(expectedEdges, inEdges);

        if (i % 1000 == 0)
          System.out.println("Checked " + expectedEdges + " edges per vertex in " + i + " vertices in " + (System.currentTimeMillis() - begin) + "ms");
      }
      System.out.println("Checked " + expectedEdges + " edges per vertex in " + i + " vertices in " + (System.currentTimeMillis() - begin) + "ms");

    } finally {
      database.commit();
      final long elapsed = System.currentTimeMillis() - begin;
      System.out.println("Check of graph finished in " + elapsed + "ms");
    }
  }
}