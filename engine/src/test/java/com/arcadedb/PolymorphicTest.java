/*
 * Copyright (c) 2018 - Arcade Analytics LTD (https://arcadeanalytics.com)
 */

package com.arcadedb;

import com.arcadedb.database.*;
import com.arcadedb.engine.PaginatedFile;
import com.arcadedb.exception.SchemaException;
import com.arcadedb.graph.ModifiableVertex;
import com.arcadedb.schema.VertexType;
import com.arcadedb.utility.FileUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.util.concurrent.atomic.AtomicInteger;

public class PolymorphicTest {
  private static final String DB_PATH = "target/database/graph";

  private static RID root;

  @BeforeEach
  public void populate() {
    FileUtils.deleteRecursively(new File(DB_PATH));

    new DatabaseFactory(DB_PATH, PaginatedFile.MODE.READ_WRITE).execute(new DatabaseFactory.POperation() {
      @Override
      public void execute(Database database) {
        //------------
        // VEHICLES VERTICES
        //------------
        VertexType vehicle = database.getSchema().createVertexType("Vehicle", 3);
        vehicle.createProperty("brand", String.class);

        VertexType motorcycle = database.getSchema().createVertexType("Motorcycle", 3);
        motorcycle.addParent("Vehicle");

        try {
          motorcycle.createProperty("brand", String.class);
          Assertions.fail("Expected to fail by creating the same property name as the parent type");
        } catch (SchemaException e) {
        }

        Assertions.assertTrue(database.getSchema().getType("Motorcycle").instanceOf("Vehicle"));
        database.getSchema().createVertexType("Car", 3).addParent("Vehicle");
        Assertions.assertTrue(database.getSchema().getType("Car").instanceOf("Vehicle"));

        database.getSchema().createVertexType("Supercar", 3).addParent("Car");
        Assertions.assertTrue(database.getSchema().getType("Supercar").instanceOf("Car"));
        Assertions.assertTrue(database.getSchema().getType("Supercar").instanceOf("Vehicle"));

        //------------
        // PEOPLE VERTICES
        //------------
        VertexType person = database.getSchema().createVertexType("Person");
        database.getSchema().createVertexType("Client").addParent(person);
        Assertions.assertTrue(database.getSchema().getType("Client").instanceOf("Person"));
        Assertions.assertFalse(database.getSchema().getType("Client").instanceOf("Vehicle"));

        //------------
        // EDGES
        //------------
        database.getSchema().createEdgeType("Drives");
        database.getSchema().createEdgeType("Owns").addParent("Drives");

        Assertions.assertTrue(database.getSchema().getType("Owns").instanceOf("Drives"));
        Assertions.assertFalse(database.getSchema().getType("Owns").instanceOf("Vehicle"));
      }
    });

    final Database db = new DatabaseFactory(DB_PATH, PaginatedFile.MODE.READ_WRITE).acquire();
    db.begin();
    try {
      final ModifiableVertex maserati = db.newVertex("Car");
      maserati.set("brand", "Maserati");
      maserati.set("type", "Ghibli");
      maserati.set("year", 2017);
      maserati.save();

      final ModifiableVertex ducati = db.newVertex("Motorcycle");
      ducati.set("brand", "Ducati");
      ducati.set("type", "Monster");
      ducati.set("year", 2015);
      ducati.save();

      final ModifiableVertex ferrari = db.newVertex("Supercar");
      ferrari.set("brand", "Ferrari");
      ferrari.set("type", "458 Italia");
      ferrari.set("year", 2014);
      ferrari.save();

      final ModifiableVertex luca = db.newVertex("Client");
      luca.set("firstName", "Luca");
      luca.set("lastName", "Skywalker");
      luca.save();

      luca.newEdge("Owns", maserati, true, "since", "2018");
      luca.newEdge("Owns", ducati, true, "since", "2016");
      luca.newEdge("Drives", ferrari, true, "since", "2018");

      db.commit();

      root = luca.getIdentity();

    } finally {
      db.close();
    }
  }

  @AfterEach
  public void drop() {
    final Database db = new DatabaseFactory(DB_PATH, PaginatedFile.MODE.READ_WRITE).acquire();
    db.drop();
  }

  @Test
  public void count() {
    final Database db2 = new DatabaseFactory(DB_PATH, PaginatedFile.MODE.READ_ONLY).acquire();
    db2.begin();
    try {
      // NON POLYMORPHIC COUNTING
      Assertions.assertEquals(0, db2.countType("Vehicle", false));
      Assertions.assertEquals(1, db2.countType("Car", false));
      Assertions.assertEquals(1, db2.countType("Supercar", false));
      Assertions.assertEquals(1, db2.countType("Motorcycle", false));

      Assertions.assertEquals(0, db2.countType("Person", false));
      Assertions.assertEquals(1, db2.countType("Client", false));

      Assertions.assertEquals(1, db2.countType("Drives", false));
      Assertions.assertEquals(2, db2.countType("Owns", false));

      // POLYMORPHIC COUNTING
      Assertions.assertEquals(3, db2.countType("Vehicle", true));
      Assertions.assertEquals(2, db2.countType("Car", true));
      Assertions.assertEquals(1, db2.countType("Supercar", true));
      Assertions.assertEquals(1, db2.countType("Motorcycle", true));

      Assertions.assertEquals(1, db2.countType("Person", true));
      Assertions.assertEquals(1, db2.countType("Client", true));

      Assertions.assertEquals(3, db2.countType("Drives", true));
      Assertions.assertEquals(2, db2.countType("Owns", true));

    } finally {
      db2.close();
    }
  }

  @Test
  public void scan() {
    final Database db2 = new DatabaseFactory(DB_PATH, PaginatedFile.MODE.READ_ONLY).acquire();
    db2.begin();
    try {
      Assertions.assertEquals(0, scanAndCountType(db2, "Vehicle", false));
      Assertions.assertEquals(1, scanAndCountType(db2, "Car", false));
      Assertions.assertEquals(1, scanAndCountType(db2, "Supercar", false));
      Assertions.assertEquals(1, scanAndCountType(db2, "Motorcycle", false));

      Assertions.assertEquals(0, scanAndCountType(db2, "Person", false));
      Assertions.assertEquals(1, scanAndCountType(db2, "Client", false));

      Assertions.assertEquals(1, scanAndCountType(db2, "Drives", false));
      Assertions.assertEquals(2, scanAndCountType(db2, "Owns", false));

      // POLYMORPHIC COUNTING
      Assertions.assertEquals(3, scanAndCountType(db2, "Vehicle", true));
      Assertions.assertEquals(2, scanAndCountType(db2, "Car", true));
      Assertions.assertEquals(1, scanAndCountType(db2, "Supercar", true));
      Assertions.assertEquals(1, scanAndCountType(db2, "Motorcycle", true));

      Assertions.assertEquals(1, scanAndCountType(db2, "Person", true));
      Assertions.assertEquals(1, scanAndCountType(db2, "Client", true));

      Assertions.assertEquals(3, scanAndCountType(db2, "Drives", true));
      Assertions.assertEquals(2, scanAndCountType(db2, "Owns", true));

    } finally {
      db2.close();
    }
  }

  private int scanAndCountType(final Database db, final String type, final boolean polymorphic) {
    // NON POLYMORPHIC COUNTING
    final AtomicInteger counter = new AtomicInteger();
    db.scanType(type, polymorphic, new DocumentCallback() {
      @Override
      public boolean onRecord(Document record) {
        Assertions.assertTrue(db.getSchema().getType(record.getType()).instanceOf(type));
        counter.incrementAndGet();
        return true;
      }
    });
    return counter.get();
  }
}