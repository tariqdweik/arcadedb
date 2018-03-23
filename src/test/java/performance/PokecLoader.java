package performance;

import com.arcadedb.database.*;
import com.arcadedb.engine.PBucket;
import com.arcadedb.engine.PFile;
import com.arcadedb.graph.PModifiableVertex;
import com.arcadedb.graph.PVertex;
import com.arcadedb.schema.PDocumentType;
import com.arcadedb.utility.PFileUtils;
import com.arcadedb.utility.PLogManager;

import java.io.*;
import java.util.zip.GZIPInputStream;

/**
 * Imports the POKEC relationships (https://snap.stanford.edu/data/soc-pokec.html)
 */
public class PokecLoader {

  private static final String DB_PATH                 = "target/database/pokec";
  private static final String POKEC_PROFILES_FILE     = "/personal/Downloads/soc-pokec-profiles.txt.gz";
  private static final String POKEC_RELATIONSHIP_FILE = "/personal/Downloads/soc-pokec-relationships.txt.gz";

  private static String[] COLUMNS = new String[] { "id", "public", "completion_percentage", "gender", "region", "last_login",
      "registration", "age", "body", "I_am_working_in_field", "spoken_languages", "hobbies", "I_most_enjoy_good_food", "pets",
      "body_type", "my_eyesight", "eye_color", "hair_color", "hair_type", "completed_level_of_education", "favourite_color",
      "relation_to_smoking", "relation_to_alcohol", "sign_in_zodiac", "on_pokec_i_am_looking_for", "love_is_for_me",
      "relation_to_casual_sex", "my_partner_should_be", "marital_status", "children", "relation_to_children", "I_like_movies",
      "I_like_watching_movie", "I_like_music", "I_mostly_like_listening_to_music", "the_idea_of_good_evening",
      "I_like_specialties_from_kitchen", "fun	I_am_going_to_concerts", "my_active_sports", "my_passive_sports", "profession",
      "I_like_books", "life_style", "music", "cars", "politics", "relationships", "art_culture", "hobbies_interests",
      "science_technologies", "computers_internet", "education", "sport", "movies", "travelling", "health", "companies_brands",
      "more" };

  public static void main(String[] args) throws Exception {
    new PokecLoader();
  }

  private PokecLoader() throws Exception {
    createSchema();

    loadProfiles();
    loadRelationships();
  }

  private void loadProfiles() throws IOException {
    InputStream fileStream = new FileInputStream(POKEC_PROFILES_FILE);
    InputStream gzipStream = new GZIPInputStream(fileStream);
    Reader decoder = new InputStreamReader(gzipStream);
    BufferedReader buffered = new BufferedReader(decoder);

    final PDatabase db = new PDatabaseFactory(DB_PATH, PFile.MODE.READ_WRITE).useParallel(true).acquire();
    ((PDatabaseParallel) db).setParallelLevel(3);
    db.begin();
    try {
      for (int i = 0; buffered.ready(); ++i) {
        final String line = buffered.readLine();
        final String[] profile = line.split("\t");

        PModifiableVertex v = db.newVertex("V");
        v.set(COLUMNS[0], Integer.parseInt(profile[0]));
        for (int c = 1; c < COLUMNS.length; ++c) {
          v.set(COLUMNS[c], profile[c]);
        }
        v.save();

        if (i % 10000 == 0) {
          PLogManager.instance().info(this, "Committing %d vertices...", i);
          db.commit();
          db.begin();
        }
      }
      db.commit();

    } finally {
      db.close();
    }
  }

  private void loadRelationships() throws IOException {
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
        final PCursor<PRID> result1 = db.lookupByKey("V", new String[] { "id" }, new Object[] { id1 });
        PVertex v1;
        if (!result1.hasNext()) {
          v1 = db.newVertex("V");
          ((PModifiableVertex) v1).set("id", id1);
          ((PModifiableVertex) v1).save();
        } else
          v1 = ((PVertex) result1.next().getRecord());

        final int id2 = Integer.parseInt(profiles[1]);
        final PCursor<PRID> result2 = db.lookupByKey("V", new String[] { "id" }, new Object[] { id2 });
        PIdentifiable v2;
        if (!result2.hasNext()) {
          v2 = db.newVertex("V");
          ((PModifiableVertex) v2).set("id", id2);
          ((PModifiableVertex) v2).save();
        } else
          v2 = result2.next();

        v1.newEdge("E", v2, true);

        if (i % 10000 == 0) {
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
        PDocumentType v = database.getSchema().createVertexType("V", 3, PBucket.DEF_PAGE_SIZE * 2);
        v.createProperty("id", Integer.class);

        for (int i = 0; i < COLUMNS.length; ++i) {
          final String column = COLUMNS[i];
          if (!v.existsProperty(column)) {
            v.createProperty(column, String.class);
          }
        }

        database.getSchema().createEdgeType("E");

        database.getSchema().createClassIndexes("V", new String[] { "id" });
      }
    });
  }
}