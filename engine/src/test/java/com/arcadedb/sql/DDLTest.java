package com.arcadedb.sql;

import com.arcadedb.BaseTest;
import com.arcadedb.database.EmbeddedDatabase;
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
                "CREATE VERTEX TYPE Car EXTENDS V; " +
                "COMMIT;  " +
                        "" );


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


    }
}