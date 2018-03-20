package performance;

import com.arcadedb.database.PDatabase;
import com.arcadedb.database.PDatabaseFactory;
import com.arcadedb.database.PVertex;
import com.arcadedb.engine.PFile;
import com.arcadedb.schema.PDocumentType;
import com.arcadedb.utility.PFileUtils;
import com.arcadedb.utility.PLogManager;

import java.io.*;
import java.util.List;
import java.util.zip.GZIPInputStream;

/**
 * Imports the POKEC relationships (https://snap.stanford.edu/data/soc-pokec.html)
 */
public class PokecLoader {

  private static final String DB_PATH                 = "target/database/pokec";
  private static final String POKEC_RELATIONSHIP_FILE = "/personal/Downloads/soc-pokec-relationships.txt.gz";

  public static void main(String[] args) throws Exception {
    new PokecLoader();
  }

  private PokecLoader() throws Exception {
    createSchema();

    InputStream fileStream = new FileInputStream(POKEC_RELATIONSHIP_FILE);
    InputStream gzipStream = new GZIPInputStream(fileStream);
    Reader decoder = new InputStreamReader(gzipStream);
    BufferedReader buffered = new BufferedReader(decoder);

    final PDatabase db = new PDatabaseFactory(DB_PATH, PFile.MODE.READ_WRITE).acquire();
    db.begin();
    try {
      for (int i = 0; buffered.ready(); ++i) {
        final String line = buffered.readLine();
        final String[] profiles = line.split("\t");

        final int id1 = Integer.parseInt(profiles[0]);
        List<PVertex> result1 = (List<PVertex>) db.lookupByKey("V", new String[] { "id" }, new Object[] { id1 });
        PVertex v1;
        if (result1.isEmpty()) {
          v1 = db.newVertex("V");
          v1.set("id", id1);
          v1.save();
        } else
          v1 = result1.get(0);

        final int id2 = Integer.parseInt(profiles[1]);
        List<PVertex> result2 = (List<PVertex>) db.lookupByKey("V", new String[] { "id" }, new Object[] { id2 });
        PVertex v2;
        if (result2.isEmpty()) {
          v2 = db.newVertex("V");
          v2.set("id", id2);
          v2.save();
        } else
          v2 = result2.get(0);

        v1.newEdge("E", v2, true);

        if (i % 1000 == 0) {
          PLogManager.instance().info(this, "Committing %d edges...", i);
          db.commit();
          db.begin();
        }

      }
      db.commit();

    } finally {
      db.close();
    }
  }

  private static void createSchema() {
    final File directory = new File(DB_PATH);
    if (directory.exists())
      PFileUtils.deleteRecursively(directory);
    else
      directory.mkdirs();

    new PDatabaseFactory(DB_PATH, PFile.MODE.READ_WRITE).execute(new PDatabaseFactory.POperation() {
      @Override
      public void execute(PDatabase database) {
        PDocumentType v = database.getSchema().createVertexType("V", 3);
        v.createProperty("id", Integer.class);

        database.getSchema().createEdgeType("E");

        database.getSchema().createClassIndexes("V", new String[] { "id" });
      }
    });
  }
}