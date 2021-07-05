/*
 * Copyright (c) - Arcade Data LTD (https://arcadedata.com)
 */

package com.arcadedb.postgresw;

import com.arcadedb.GlobalConfiguration;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.postgresql.util.PSQLException;

import java.sql.*;
import java.util.Properties;

public class PostgresWTest extends BaseGraphServerTest {

  @BeforeEach
  @Override
  public void beginTest() {
    GlobalConfiguration.SERVER_PLUGINS.setValue("Postgres:com.arcadedb.postgresw.PostgresWrapperPlugin");
    super.beginTest();
  }

  @AfterEach
  @Override
  public void endTest() {
    GlobalConfiguration.SERVER_PLUGINS.setValue("");
    super.endTest();
  }

  @Test
  public void testSet() throws InterruptedException, ClassNotFoundException, SQLException {
    //Thread.sleep(1000000);

    Class.forName("org.postgresql.Driver");

    String url = "jdbc:postgresql://localhost/" + getDatabaseName();
    Properties props = new Properties();
    props.setProperty("user", "root");
    props.setProperty("password", "root");
    props.setProperty("ssl", "false");
    Connection conn = DriverManager.getConnection(url, props);

    Statement st = conn.createStatement();
    try {
      st.executeQuery("SELECT * FROM V");
      Assertions.fail("The query should go in error");
    } catch (PSQLException e) {
    }
    st.close();

    st = conn.createStatement();
    st.executeQuery("create vertex type V");
    st.close();

    st = conn.createStatement();
    st.executeQuery("create vertex V set name = 'Jay', lastName = 'Miner'");
    st.close();

    PreparedStatement pst = conn.prepareStatement("create vertex V set name = ?, lastName = ?");
    pst.setString(1, "Rocky");
    pst.setString(2, "Balboa");
    pst.execute();

    pst.close();

    st = conn.createStatement();
    ResultSet rs = st.executeQuery("SELECT * FROM V");

    Assertions.assertTrue(!rs.isAfterLast());

    int i = 0;
    while (rs.next()) {
      if (rs.getString(1).equalsIgnoreCase("Jay")) {
        Assertions.assertEquals("Jay", rs.getString(1));
        Assertions.assertEquals("Miner", rs.getString(2));
        ++i;
      } else if (rs.getString(1).equalsIgnoreCase("Rocky")) {
        Assertions.assertEquals("Rocky", rs.getString(1));
        Assertions.assertEquals("Balboa", rs.getString(2));
        ++i;
      } else
        Assertions.fail("Unknown value");
    }

    Assertions.assertEquals(2, i);

    rs.close();
    st.close();
  }

  protected String getDatabaseName() {
    return "postgresdb";
  }
}