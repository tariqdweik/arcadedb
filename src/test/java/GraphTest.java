import com.arcadedb.database.*;
import com.arcadedb.engine.PFile;
import com.arcadedb.utility.PFileUtils;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.util.Iterator;

public class GraphTest {
  private static final int    TOT               = 10000;
  private static final String VERTEX1_TYPE_NAME = "V1";
  private static final String VERTEX2_TYPE_NAME = "V2";
  private static final String EDGE1_TYPE_NAME   = "E1";
  private static final String EDGE2_TYPE_NAME   = "E2";
  private static final String DB_PATH           = "target/database/graph";

  @BeforeAll
  public static void populate() {
    createSchema();
  }

  @AfterAll
  public static void drop() {
    final PDatabase db = new PDatabaseFactory(DB_PATH, PFile.MODE.READ_WRITE).acquire();
    db.drop();
  }

  @Test
  public void testSimpleGraph() throws IOException {
    PRID root;

    final PDatabase db = new PDatabaseFactory(DB_PATH, PFile.MODE.READ_WRITE).acquire();
    db.begin();
    try {
      final PVertex v1 = db.newVertex(VERTEX1_TYPE_NAME);
      v1.set("name", "V1");
      v1.save();

      final PVertex v2 = db.newVertex(VERTEX2_TYPE_NAME);
      v2.set("name", "V2");
      v2.save();

      v1.newEdge(EDGE1_TYPE_NAME, v2, true);

      final PVertex v3 = db.newVertex(VERTEX2_TYPE_NAME);
      v3.set("name", "V3");
      v3.save();

      v2.newEdge(EDGE2_TYPE_NAME, v3, true);
      v1.newEdge(EDGE2_TYPE_NAME, v3, true);

      db.commit();

      root = v1.getIdentity();

    } finally {
      db.close();
    }

    final PDatabase db2 = new PDatabaseFactory(DB_PATH, PFile.MODE.READ_ONLY).acquire();
    db2.begin();
    try {

      Assertions.assertEquals(1, db2.countType(VERTEX1_TYPE_NAME));
      Assertions.assertEquals(2, db2.countType(VERTEX2_TYPE_NAME));

      final PVertex v1 = (PVertex) db2.lookupByRID(root);
      Assertions.assertNotNull(v1);

      // TEST CONNECTED VERTICES
      Assertions.assertEquals(VERTEX1_TYPE_NAME, v1.getType());
      Assertions.assertEquals("V1", v1.get("name"));

      final Iterator<PVertex> vertices2level = v1.getVertices(PVertex.DIRECTION.OUT, EDGE1_TYPE_NAME);
      Assertions.assertNotNull(vertices2level);
      Assertions.assertTrue(vertices2level.hasNext());

      final PVertex v2 = vertices2level.next();
      Assertions.assertNotNull(v2);

      Assertions.assertEquals(VERTEX2_TYPE_NAME, v2.getType());
      Assertions.assertEquals("V2", v2.get("name"));

      final Iterator<PVertex> vertices2level2 = v1.getVertices(PVertex.DIRECTION.OUT, EDGE2_TYPE_NAME);
      Assertions.assertNotNull(vertices2level2);
      Assertions.assertTrue(vertices2level2.hasNext());

      final PVertex v3 = vertices2level2.next();
      Assertions.assertNotNull(v3);

      Assertions.assertEquals(VERTEX2_TYPE_NAME, v3.getType());
      Assertions.assertEquals("V3", v3.get("name"));

      final Iterator<PVertex> vertices3level = v2.getVertices(PVertex.DIRECTION.OUT, EDGE2_TYPE_NAME);
      Assertions.assertNotNull(vertices3level);
      Assertions.assertTrue(vertices3level.hasNext());

      final PVertex v32 = vertices3level.next();
      Assertions.assertNotNull(v32);
      Assertions.assertEquals(VERTEX2_TYPE_NAME, v32.getType());
      Assertions.assertEquals("V3", v32.get("name"));


      // TEST CONNECTED EDGES
      final Iterator<PEdge> edges = v1.getEdges(PVertex.DIRECTION.OUT, EDGE1_TYPE_NAME);
      Assertions.assertNotNull(edges);

    } finally {
      db2.close();
    }
  }

  private static void createSchema() {
    PFileUtils.deleteRecursively(new File(DB_PATH));

    new PDatabaseFactory(DB_PATH, PFile.MODE.READ_WRITE).execute(new PDatabaseFactory.POperation() {
      @Override
      public void execute(PDatabase database) {
        Assertions.assertFalse(database.getSchema().existsType(VERTEX1_TYPE_NAME));
        database.getSchema().createVertexType(VERTEX1_TYPE_NAME, 3);

        Assertions.assertFalse(database.getSchema().existsType(VERTEX2_TYPE_NAME));
        database.getSchema().createVertexType(VERTEX2_TYPE_NAME, 3);

        database.getSchema().createEdgeType(EDGE1_TYPE_NAME);
        database.getSchema().createEdgeType(EDGE2_TYPE_NAME);
      }
    });
  }
}