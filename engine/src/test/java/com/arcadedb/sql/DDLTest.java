package com.arcadedb.sql;

import com.arcadedb.BaseTest;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

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
            db.command("sql", "CREATE VERTEX TYPE Car EXTENDS V");

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
    }


    @Test
    void rename() {


        database.execute("sql",
                "BEGIN;" +
                        "CREATE VERTEX TYPE Person EXTENDS V; " +
                        "CREATE PROPERTY Person.name STRING;" +
                        "CREATE VERTEX TYPE Car EXTENDS V; " +
                        "CREATE PROPERTY Car.model STRING;" +
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
                    .forEach(r -> System.out.println(r));

        });

        System.out.println("----------");
        database.transaction(db -> {
            db.query("sql", "select from schema:indexes")
                    .stream()
                    .forEach(r -> System.out.println(r));

        });

    }
}