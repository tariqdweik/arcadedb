package com.arcadedb.server;

import com.arcadedb.database.PDatabase;
import com.arcadedb.database.PDatabaseFactory;
import com.arcadedb.engine.PPaginatedFile;
import com.arcadedb.utility.PLogManager;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URL;

public class HTTPGraphTest extends BaseGraphServerTest {
  private PHttpServer server;

  @BeforeEach
  public void populate() {
    super.populate();

    final PHttpServerConfiguration config = new PHttpServerConfiguration();
    config.databaseDirectory = "target/database";
    server = new PHttpServer(config);

    new Thread(new Runnable() {
      @Override
      public void run() {
        server.start();
        PLogManager.instance().info(this, "Test Server is down");
      }
    }).start();

    try {
      Thread.sleep(1000);
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
  }

  @AfterEach
  public void drop() {
    server.close();

    final PDatabase db = new PDatabaseFactory(BaseGraphServerTest.DB_PATH, PPaginatedFile.MODE.READ_WRITE).acquire();
    db.drop();
  }

  @Test
  public void checkQuery() throws IOException {
    HttpURLConnection connection = (HttpURLConnection) new URL("http://127.0.0.1:2480/command/graph/select%20from%20V1%20limit%201")
        .openConnection();

    connection.setRequestMethod("POST");
    connection.connect();

    try {
      final String response = readResponse(connection);

      PLogManager.instance().info(this, "Response: ", response);

      Assertions.assertEquals(200, connection.getResponseCode());

      Assertions.assertEquals("OK", connection.getResponseMessage());

      Assertions.assertTrue(response.contains("V1"));

    } finally {
      connection.disconnect();
    }
  }

  @Test
  public void checkRecordLoading() throws IOException {
    HttpURLConnection connection = (HttpURLConnection) new URL(
        "http://127.0.0.1:2480/record/graph/" + BaseGraphServerTest.root.getIdentity().toString().substring(1)).openConnection();

    connection.setRequestMethod("GET");
    connection.connect();

    try {
      final String response = readResponse(connection);

      PLogManager.instance().info(this, "Response: ", response);

      Assertions.assertEquals(200, connection.getResponseCode());

      Assertions.assertEquals("OK", connection.getResponseMessage());

      Assertions.assertTrue(response.contains("V1"));

    } finally {
      connection.disconnect();
    }
  }
//
//  @Test
//  public void checkRecordCreate() throws IOException {
//    HttpURLConnection connection = (HttpURLConnection) new URL("http://127.0.0.1:2480/record/graph/{\"name\":\"Luca\"}")
//        .openConnection();
//
//    connection.setRequestMethod("POST");
//    connection.connect();
//
//    try {
//      final String response = readResponse(connection);
//
//      PLogManager.instance().info(this, "Response: ", response);
//
//      Assertions.assertEquals(200, connection.getResponseCode());
//
//      Assertions.assertEquals("OK", connection.getResponseMessage());
//
//      Assertions.assertTrue(response.contains("V1"));
//
//    } finally {
//      connection.disconnect();
//    }
//  }
}