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
import com.arcadedb.graph.MutableVertex;
import com.arcadedb.graph.Vertex;
import com.arcadedb.index.IndexCursor;
import com.arcadedb.log.LogManager;
import com.arcadedb.log.Logger;
import com.arcadedb.schema.EdgeType;
import com.arcadedb.schema.SchemaImpl;
import com.arcadedb.schema.VertexType;

import java.util.logging.Level;

public class PerformanceInsertGraphIndexTest extends BaseTest {
  private static final int    VERTICES         = 100_000;
  private static final int    EDGES_PER_VERTEX = 1_000;
  private static final String VERTEX_TYPE_NAME = "Person";
  private static final String EDGE_TYPE_NAME   = "Friend";
  private static final int    PARALLEL         = 3;

  public static void main(String[] args) {
    PerformanceTest.clean();
    new PerformanceInsertGraphIndexTest().run();
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

  private void run() {
    if (!database.getSchema().existsType(VERTEX_TYPE_NAME)) {
      database.begin();

      final VertexType type = database.getSchema().createVertexType(VERTEX_TYPE_NAME, PARALLEL);
      type.createProperty("id", Long.class);

      final EdgeType edge = database.getSchema().createEdgeType(EDGE_TYPE_NAME, PARALLEL);

      database.getSchema().createTypeIndex(SchemaImpl.INDEX_TYPE.LSM_TREE, false, VERTEX_TYPE_NAME, new String[] { "id" }, 5000000);

      database.commit();

      System.out.println("Start inserting " + VERTICES + " vertices...");

      long startOfTest = System.currentTimeMillis();

      try {
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

      System.out.println("Start inserting " + EDGES_PER_VERTEX + " edges per vertex...");

      final Vertex[] cachedVertices = new Vertex[VERTICES];

      System.out.println("Loading " + VERTICES + " in RAM...");
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
            if (counter % 1_000_000 == 0)
              System.out.println("Loaded " + counter + " vertices in " + (System.currentTimeMillis() - begin) + "ms");
          }
          System.out.println("Loaded " + counter + " vertices in " + (System.currentTimeMillis() - begin) + "ms");
        } finally {
          final long elapsed = System.currentTimeMillis() - begin;
          System.out.println("Loaded all vertices in RAM in " + elapsed + "ms -> " + (VERTICES / (elapsed / 1000F)) + " ops/sec");
        }
      });

      System.out.println("Creating " + EDGES_PER_VERTEX + " edges per vertex on all " + VERTICES + " vertices");

      database.async().close();

      database.begin();
      final long begin = System.currentTimeMillis();
      try {
        int counter = 0;
        for (; counter < VERTICES; ++counter) {
          int edges = 0;
          for (int i = 0; i < cachedVertices.length; i++) {
            if (i != counter) {
              cachedVertices[counter].newEdge(EDGE_TYPE_NAME, cachedVertices[i], true);
            }
          }

          if (counter % 10 == 0)
            System.out
                .println("Created " + EDGES_PER_VERTEX + " edges per vertex in " + counter + " vertices in " + (System.currentTimeMillis() - begin) + "ms");

          if (counter % 10 == 0) {
            database.commit();
            database.begin();
          }

          if (++edges > EDGES_PER_VERTEX)
            break;
        }
        System.out.println("Created " + EDGES_PER_VERTEX + " edges per vertex in " + counter + " vertices in " + (System.currentTimeMillis() - begin) + "ms");

      } finally {
        database.commit();
        final long elapsed = System.currentTimeMillis() - begin;
        System.out.println("Creation of edges finished in " + elapsed + "ms");
      }
    }

    database.close();
  }
}