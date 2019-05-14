package com.arcadedb.sql;

import com.arcadedb.BaseTest;
import com.arcadedb.schema.Property;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import sun.applet.resources.MsgAppletViewer;

import java.util.HashMap;
import java.util.Map;
import java.util.stream.IntStream;

public class DDLTest extends BaseTest {
    @Override
    protected void beginTest() {

        database.transaction(database -> database.command("sql", "CREATE VERTEX TYPE V"));

    }


    @Test
    void testCreateVertexTypes() {

        database.transaction(db -> {

            db.command("sql", "CREATE VERTEX TYPE Person EXTENDS V");
            db.command("sql", "CREATE PROPERTY Person.id INTEGER");
            db.command("sql", "CREATE PROPERTY Person.name STRING");
            db.command("sql", "CREATE INDEX Person.id ON Person (id) UNIQUE");
            db.command("sql", "CREATE VERTEX TYPE Car EXTENDS V");

//            IntStream.range(0, 1000)
//                    .forEach(i -> {
//                        db.command("sql", "INSERT INTO Person set id=?,  name=?, surname=?", i, "Jay", "Miner" + i);
//                        db.command("sql", "INSERT INTO Car set id=?,  brand=?, model=?", i, "Ferrari", "450" + i);
//                    });
            IntStream.range(0, 1000)
                    .forEach(i -> {
                        Map<String,Object> args = new HashMap<>();
                        args.put(":id", i);
                        args.put(":name", "Jay");
                        args.put(":surname", "Miner"+i);
                        db.command("sql", "INSERT INTO Person set id=:id,  name=:name, surname=:surname", args);
                        db.command("sql", "INSERT INTO Car set id=?,  brand=?, model=?", i, "Ferrari", "450" + i);
                    });

        });


        database.transaction(db -> {

            final Long persons = db.command("sql", "SELECT count(*) as persons FROM Person ")
                    .next().<Long>getProperty("persons");

            Assertions.assertEquals(1000, persons);

            final Long cars = db.command("sql", "SELECT count(*) as cars FROM Car")
                    .next().<Long>getProperty("cars");

            Assertions.assertEquals(1000, cars);

            final Long vs = db.command("sql", "SELECT count(*) as vs FROM V")
                    .next().<Long>getProperty("vs");

            Assertions.assertEquals(2000, vs);
        });
    }

    @Test
    void testCreateVertexTypesExplicitTransaction() {

        database.command("sql", "BEGIN");
        database.command("sql", "CREATE VERTEX TYPE Person EXTENDS V");
        database.command("sql", "CREATE PROPERTY Person.id INTEGER");
        database.command("sql", "CREATE PROPERTY Person.name STRING");
        database.command("sql", "CREATE INDEX Person.id ON Person (id) UNIQUE");
        database.command("sql", "CREATE VERTEX TYPE Car EXTENDS V");
        database.command("sql", "COMMIT");

        database.command("sql", "BEGIN");
        IntStream.range(0, 1000)
                .forEach(i -> {
                    database.command("sql", "INSERT INTO Person set id=?,  name=?, surname=?", i, "Jay", "Miner" + i);
                    database.command("sql", "INSERT INTO Car set id=?,  brand=?, model=?", i, "Ferrari", "450" + i);
                });
        database.command("sql", "COMMIT");


        database.transaction(db -> {

            final Long persons = db.query("sql", "SELECT count(*) as persons FROM Person ")
                    .next().<Long>getProperty("persons");

            Assertions.assertEquals(1000, persons);

            final Long cars = db.query("sql", "SELECT count(*) as cars FROM Car")
                    .next().<Long>getProperty("cars");

            Assertions.assertEquals(1000, cars);

            final Long vs = db.query("sql", "SELECT count(*) as vs FROM V")
                    .next().<Long>getProperty("vs");

            Assertions.assertEquals(2000, vs);
        });


        database.transaction(db -> {
            db.query("sql", "select from schema:types")
                    .stream()
                    .forEach(r -> {
                        System.out.println(r);

                        System.out.println("r.getProperty(\"properties\") = " + r.getProperty("properties"));
                        final Map<String, Property> properties = r.getProperty("properties");

                        System.out.println("properties = " + properties);
                    });

        });

    }


    @Test
    void rename() {


        database.execute("sql",
                "BEGIN;" +
                        "CREATE VERTEX TYPE Person EXTENDS V; " +
                        "CREATE PROPERTY Person.name STRING;" +
                        "CREATE PROPERTY Person.id INTEGER;" +
                        "CREATE INDEX Person.id ON Person (id) UNIQUE;" +
                        "CREATE VERTEX TYPE Car EXTENDS V; " +
                        "CREATE PROPERTY Car.id INTEGER;" +
                        "CREATE PROPERTY Car.model STRING;" +
                        "CREATE INDEX Car.id ON Car (id) UNIQUE;" +
                        "COMMIT;  " +
                        "");


        database.transaction(db -> {


            IntStream.range(0, 1000)
                    .forEach(i -> {
                        db.command("sql", "INSERT INTO Person set id=?,  name=?, surname=?", i, "Jay", "Miner" + i);
                        db.command("sql", "INSERT INTO Car set id=?,  brand=?, model=?", i, "Ferrari", "450" + i);
                    });

        });


        database.transaction(db -> {

            final Long persons = db.command("sql", "SELECT count(*) as persons FROM Person ")
                    .next().<Long>getProperty("persons");

            Assertions.assertEquals(1000, persons);

            final Long cars = db.command("sql", "SELECT count(*) as cars FROM Car")
                    .next().<Long>getProperty("cars");

            Assertions.assertEquals(1000, cars);

            final Long vs = db.command("sql", "SELECT count(*) as vs FROM V")
                    .next().<Long>getProperty("vs");

            Assertions.assertEquals(2000, vs);
        });


        database.transaction(db -> {
            db.query("sql", "select from schema:types")
                    .stream()
                    .forEach(r -> {
                        System.out.println(r);

                        System.out.println("r.getProperty(\"properties\") = " + r.getProperty("properties"));
//                        final Map<String, Property> properties = r.getProperty("properties");
//
//                        System.out.println("properties = " + properties);
                    });

        });

//        System.out.println("----------");
//        database.transaction(db -> {
//            db.query("sql", "select from schema:database")
//                    .stream()
//                    .forEach(r -> System.out.println(r));
//
//        });
        System.out.println("----------");
        database.transaction(db -> {
            db.query("sql", "select from schema:indexes")
                    .stream()
                    .forEach(r -> System.out.println(r));

        });

    }
}