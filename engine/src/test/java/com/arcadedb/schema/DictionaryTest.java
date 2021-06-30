/*
 * Copyright (c) - Arcade Data LTD (https://arcadedata.com)
 */

package com.arcadedb.schema;

import com.arcadedb.BaseTest;
import com.arcadedb.database.Document;
import com.arcadedb.database.MutableDocument;
import com.arcadedb.sql.executor.ResultSet;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class DictionaryTest extends BaseTest {
  @Test
  public void updateName() {
    database.transaction((database) -> {
      Assertions.assertFalse(database.getSchema().existsType("V"));

      final DocumentType type = database.getSchema().createDocumentType("V", 3);
      type.createProperty("id", Integer.class);
      type.createProperty("name", String.class);

      for (int i = 0; i < 10; ++i) {
        final MutableDocument v = database.newDocument("V");
        v.set("id", i);
        v.set("name", "Jay");
        v.set("surname", "Miner");
        v.save();
      }
    });

    Assertions.assertEquals(4, database.getSchema().getDictionary().getDictionaryMap().size());

    database.transaction((database) -> {
      Assertions.assertTrue(database.getSchema().existsType("V"));

      final MutableDocument v = database.newDocument("V");
      v.set("id", 10);
      v.set("name", "Jay");
      v.set("surname", "Miner");
      v.set("newProperty", "newProperty");
      v.save();
    });

    Assertions.assertEquals(5, database.getSchema().getDictionary().getDictionaryMap().size());

    database.transaction((database) -> {
      Assertions.assertTrue(database.getSchema().existsType("V"));
      database.getSchema().getDictionary().updateName("name", "firstName");
    });

    Assertions.assertEquals(5, database.getSchema().getDictionary().getDictionaryMap().size());

    database.transaction((database) -> {
      final ResultSet iter = database.query("sql", "select from V order by id asc");

      int i = 0;
      while (iter.hasNext()) {
        final Document d = (Document) iter.next().getRecord().get();

        Assertions.assertEquals(i, d.getInteger("id"));
        Assertions.assertEquals("Jay", d.getString("firstName"));
        Assertions.assertEquals("Miner", d.getString("surname"));

        if (i == 10)
          Assertions.assertEquals("newProperty", d.getString("newProperty"));
        else
          Assertions.assertNull(d.getString("newProperty"));

        Assertions.assertNull(d.getString("name"));

        ++i;
      }

      Assertions.assertEquals(11, i);
    });

    try {
      database.transaction((database) -> {
        Assertions.assertTrue(database.getSchema().existsType("V"));
        database.getSchema().getDictionary().updateName("V", "V2");
      });
      Assertions.fail();
    } catch (Exception e) {
    }
  }
}