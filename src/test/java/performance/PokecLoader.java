package performance;

import com.arcadedb.database.PDatabase;
import com.arcadedb.database.PDatabaseFactory;
import com.arcadedb.engine.PBucket;
import com.arcadedb.engine.PPaginatedFile;
import com.arcadedb.graph.PModifiableVertex;
import com.arcadedb.index.PIndexLSM;
import com.arcadedb.schema.PDocumentType;
import com.arcadedb.utility.PFileUtils;
import com.arcadedb.utility.PLogManager;

import java.io.*;

/**
 * Imports the POKEC relationships (https://snap.stanford.edu/data/soc-pokec.html)
 */
public class PokecLoader {

  private static final String DB_PATH                 = "target/database/pokec";
  private static final String POKEC_PROFILES_FILE     = "/personal/Downloads/soc-pokec-profiles.txt";
  private static final String POKEC_RELATIONSHIP_FILE = "/personal/Downloads/soc-pokec-relationships.txt";
  private static final int    PARALLEL_LEVEL          = 2;

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
    final File directory = new File(DB_PATH);
    if (directory.exists())
      PFileUtils.deleteRecursively(directory);
    else
      directory.mkdirs();

    final PDatabase db = new PDatabaseFactory(DB_PATH, PPaginatedFile.MODE.READ_WRITE).acquire();
    try {
      createSchema(db);
      loadProfiles(db);
      loadRelationships(db);
    } finally {
      db.close();
    }
  }

  private void loadProfiles(final PDatabase db) throws IOException {
    InputStream fileStream = new FileInputStream(POKEC_PROFILES_FILE);
//    InputStream gzipStream = new GZIPInputStream(fileStream);
    Reader decoder = new InputStreamReader(fileStream);
    BufferedReader buffered = new BufferedReader(decoder);

    db.asynch().setParallelLevel(PARALLEL_LEVEL);

    for (int i = 0; buffered.ready(); ++i) {
      final String line = buffered.readLine();
      final String[] profile = line.split("\t");

      final PModifiableVertex v = db.newVertex("V");
      final int id = Integer.parseInt(profile[0]);
      v.set(COLUMNS[0], id);
      for (int c = 1; c < COLUMNS.length; ++c) {
        v.set(COLUMNS[c], profile[c]);
      }

      db.asynch().createRecord(v);

      if (i % 20000 == 0) {
        PLogManager.instance().info(this, "Inserted %d vertices...", i);
      }
    }
  }

  private void loadRelationships(PDatabase db) throws IOException {
    InputStream fileStream = new FileInputStream(POKEC_RELATIONSHIP_FILE);
//    InputStream gzipStream = new GZIPInputStream(fileStream);
    Reader decoder = new InputStreamReader(fileStream);
    BufferedReader buffered = new BufferedReader(decoder);

    db.asynch().waitCompletion();

    db.begin();

    for (int i = 0; buffered.ready(); ++i) {
      final String line = buffered.readLine();
      final String[] profiles = line.split("\t");

      final int id1 = Integer.parseInt(profiles[0]);
      final int id2 = Integer.parseInt(profiles[1]);

      db.asynch()
          .newEdgeByKeys("V", new String[] { "id" }, new Object[] { id1 }, "V", new String[] { "id" }, new Object[] { id2 }, false,
              "E", true);

      if (i % 20000 == 0) {
        PLogManager.instance().info(this, "Committing %d edges...", i);
        db.commit();
        db.begin();
      }
    }
    db.commit();
  }

  private static void createSchema(final PDatabase db) {
    db.begin();
    PDocumentType v = db.getSchema().createVertexType("V", PARALLEL_LEVEL, PBucket.DEF_PAGE_SIZE * 2);
    v.createProperty("id", Integer.class);

    for (int i = 0; i < COLUMNS.length; ++i) {
      final String column = COLUMNS[i];
      if (!v.existsProperty(column)) {
        v.createProperty(column, String.class);
      }
    }

    db.getSchema().createEdgeType("E");

    db.getSchema().createClassIndexes("V", new String[] { "id" }, PIndexLSM.DEF_PAGE_SIZE * 20);
    db.commit();
  }
}