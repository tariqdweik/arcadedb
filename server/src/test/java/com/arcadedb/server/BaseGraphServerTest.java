package com.arcadedb.server;

import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.database.RID;
import com.arcadedb.engine.PaginatedFile;
import com.arcadedb.graph.ModifiableEdge;
import com.arcadedb.graph.ModifiableVertex;
import com.arcadedb.utility.FileUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.util.HashMap;
import java.util.Map;
import java.util.Scanner;

public abstract class BaseGraphServerTest {
  protected static final String VERTEX1_TYPE_NAME = "V1";
  protected static final String VERTEX2_TYPE_NAME = "V2";
  protected static final String EDGE1_TYPE_NAME   = "E1";
  protected static final String EDGE2_TYPE_NAME   = "E2";
  protected static final String DB_PATH           = "target/database/graph";

  protected static RID root;

  @BeforeEach
  public void populate() {
    FileUtils.deleteRecursively(new File(DB_PATH));

    new DatabaseFactory(DB_PATH, PaginatedFile.MODE.READ_WRITE).execute(new DatabaseFactory.POperation() {
      @Override
      public void execute(Database database) {
        Assertions.assertFalse(database.getSchema().existsType(VERTEX1_TYPE_NAME));
        database.getSchema().createVertexType(VERTEX1_TYPE_NAME, 3);

        Assertions.assertFalse(database.getSchema().existsType(VERTEX2_TYPE_NAME));
        database.getSchema().createVertexType(VERTEX2_TYPE_NAME, 3);

        database.getSchema().createEdgeType(EDGE1_TYPE_NAME);
        database.getSchema().createEdgeType(EDGE2_TYPE_NAME);
      }
    });

    final Database db = new DatabaseFactory(DB_PATH, PaginatedFile.MODE.READ_WRITE).acquire();
    db.begin();
    try {
      final ModifiableVertex v1 = db.newVertex(VERTEX1_TYPE_NAME);
      v1.set("name", VERTEX1_TYPE_NAME);
      v1.save();

      final ModifiableVertex v2 = db.newVertex(VERTEX2_TYPE_NAME);
      v2.set("name", VERTEX2_TYPE_NAME);
      v2.save();

      // CREATION OF EDGE PASSING PARAMS AS VARARGS
      ModifiableEdge e1 = (ModifiableEdge) v1.newEdge(EDGE1_TYPE_NAME, v2, true, "name", "E1");
      Assertions.assertEquals(e1.getOut(), v1);
      Assertions.assertEquals(e1.getIn(), v2);

      final ModifiableVertex v3 = db.newVertex(VERTEX2_TYPE_NAME);
      v3.set("name", "V3");
      v3.save();

      Map<String, Object> params = new HashMap<>();
      params.put("name", "E2");

      // CREATION OF EDGE PASSING PARAMS AS MAP
      ModifiableEdge e2 = (ModifiableEdge) v2.newEdge(EDGE2_TYPE_NAME, v3, true, params);
      Assertions.assertEquals(e2.getOut(), v2);
      Assertions.assertEquals(e2.getIn(), v3);

      ModifiableEdge e3 = (ModifiableEdge) v1.newEdge(EDGE2_TYPE_NAME, v3, true);
      Assertions.assertEquals(e3.getOut(), v1);
      Assertions.assertEquals(e3.getIn(), v3);

      db.commit();

      root = v1.getIdentity();

    } finally {
      db.close();
    }
  }

  @AfterEach
  public void drop() {
    final Database db = new DatabaseFactory(DB_PATH, PaginatedFile.MODE.READ_WRITE).acquire();
    db.drop();
  }

  protected String readResponse(final HttpURLConnection connection) throws IOException {
    InputStream in = connection.getInputStream();
    Scanner scanner = new Scanner(in);

    final StringBuilder buffer = new StringBuilder();

    while (scanner.hasNext()) {
      buffer.append(scanner.next().replace('\n', ' '));
    }

    return buffer.toString();
  }
}