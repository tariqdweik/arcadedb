/*
 * Copyright (c) 2018 - Arcade Analytics LTD (https://arcadeanalytics.com)
 */

package com.arcadedb.serializer;

import com.arcadedb.database.Binary;
import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.database.ModifiableDocument;
import com.arcadedb.engine.Bucket;
import com.arcadedb.engine.PaginatedFile;
import com.arcadedb.utility.FileUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class SerializerTest {

  public static final String DB_PATH = "./target/arcadedb/testdb";

  @Test
  public void testVarNumber() {
    final Binary binary = new Binary();
    binary.putNumber(0);
    binary.putNumber(3);
    binary.putNumber(Short.MIN_VALUE);
    binary.putNumber(Short.MAX_VALUE);
    binary.putNumber(Integer.MIN_VALUE);
    binary.putNumber(Integer.MAX_VALUE);
    binary.putNumber(Long.MIN_VALUE);
    binary.putNumber(Long.MAX_VALUE);

    binary.putShort((short) 0);
    binary.putShort(Short.MIN_VALUE);
    binary.putShort(Short.MAX_VALUE);

    binary.putInt(0);
    binary.putInt(Integer.MIN_VALUE);
    binary.putInt(Integer.MAX_VALUE);

    binary.putLong(0l);
    binary.putLong(Long.MIN_VALUE);
    binary.putLong(Long.MAX_VALUE);

    binary.flip();

    final ByteBuffer dBuffer = ByteBuffer.allocate(1024);
    final Binary buffer = new Binary(dBuffer);
    dBuffer.put(binary.toByteArray());

    binary.rewind();
    buffer.rewind();

    Assertions.assertEquals(0, binary.getNumber());
    Assertions.assertEquals(0, buffer.getNumber());

    Assertions.assertEquals(3, binary.getNumber());
    Assertions.assertEquals(3, buffer.getNumber());

    Assertions.assertEquals(Short.MIN_VALUE, binary.getNumber());
    Assertions.assertEquals(Short.MIN_VALUE, buffer.getNumber());

    Assertions.assertEquals(Short.MAX_VALUE, binary.getNumber());
    Assertions.assertEquals(Short.MAX_VALUE, buffer.getNumber());

    Assertions.assertEquals(Integer.MIN_VALUE, binary.getNumber());
    Assertions.assertEquals(Integer.MIN_VALUE, buffer.getNumber());

    Assertions.assertEquals(Integer.MAX_VALUE, binary.getNumber());
    Assertions.assertEquals(Integer.MAX_VALUE, buffer.getNumber());

    Assertions.assertEquals(Long.MIN_VALUE, binary.getNumber());
    Assertions.assertEquals(Long.MIN_VALUE, buffer.getNumber());

    Assertions.assertEquals(Long.MAX_VALUE, binary.getNumber());
    Assertions.assertEquals(Long.MAX_VALUE, buffer.getNumber());

    Assertions.assertEquals(0, binary.getShort());
    Assertions.assertEquals(0, buffer.getShort());

    Assertions.assertEquals(Short.MIN_VALUE, binary.getShort());
    Assertions.assertEquals(Short.MIN_VALUE, buffer.getShort());

    Assertions.assertEquals(Short.MAX_VALUE, binary.getShort());
    Assertions.assertEquals(Short.MAX_VALUE, buffer.getShort());

    Assertions.assertEquals(0, binary.getInt());
    Assertions.assertEquals(0, buffer.getInt());

    Assertions.assertEquals(Integer.MIN_VALUE, binary.getInt());
    Assertions.assertEquals(Integer.MIN_VALUE, buffer.getInt());

    Assertions.assertEquals(Integer.MAX_VALUE, binary.getInt());
    Assertions.assertEquals(Integer.MAX_VALUE, buffer.getInt());

    Assertions.assertEquals(0l, binary.getLong());
    Assertions.assertEquals(0l, buffer.getLong());

    Assertions.assertEquals(Long.MIN_VALUE, binary.getLong());
    Assertions.assertEquals(Long.MIN_VALUE, buffer.getLong());

    Assertions.assertEquals(Long.MAX_VALUE, binary.getLong());
    Assertions.assertEquals(Long.MAX_VALUE, buffer.getLong());
  }

  @Test
  public void testLiteralPropertiesInDocument() {
    FileUtils.deleteRecursively(new File(DB_PATH));

    final BinarySerializer serializer = new BinarySerializer();

    new DatabaseFactory(DB_PATH, PaginatedFile.MODE.READ_WRITE).execute(new DatabaseFactory.POperation() {
      @Override
      public void execute(Database database) {
        database.getSchema().createDocumentType("Test");
        database.commit();

        database.begin();

        final ModifiableDocument v = database.newDocument("Test");
        v.set("minInt", Integer.MIN_VALUE);
        v.set("maxInt", Integer.MAX_VALUE);
        v.set("minLong", Long.MIN_VALUE);
        v.set("maxLong", Long.MAX_VALUE);
        v.set("minShort", Short.MIN_VALUE);
        v.set("maxShort", Short.MAX_VALUE);
        v.set("minByte", Byte.MIN_VALUE);
        v.set("maxByte", Byte.MAX_VALUE);
        v.set("decimal", new BigDecimal(9876543210.0123456789));
        v.set("string", "Miner");

        final Binary buffer = serializer.serialize(database, v, -1);

        final ByteBuffer buffer2 = ByteBuffer.allocate(Bucket.DEF_PAGE_SIZE);
        buffer2.put(buffer.toByteArray());
        buffer2.flip();

        Binary buffer3 = new Binary(buffer2);
        buffer3.getByte(); // SKIP RECORD TYPE
        Map<String, Object> record2 = serializer.deserializeProperties(database, buffer3);

        Assertions.assertEquals(Integer.MIN_VALUE, record2.get("minInt"));
        Assertions.assertEquals(Integer.MAX_VALUE, record2.get("maxInt"));

        Assertions.assertEquals(Long.MIN_VALUE, record2.get("minLong"));
        Assertions.assertEquals(Long.MAX_VALUE, record2.get("maxLong"));

        Assertions.assertEquals(Short.MIN_VALUE, record2.get("minShort"));
        Assertions.assertEquals(Short.MAX_VALUE, record2.get("maxShort"));

        Assertions.assertEquals(Byte.MIN_VALUE, record2.get("minByte"));
        Assertions.assertEquals(Byte.MAX_VALUE, record2.get("maxByte"));

        Assertions.assertTrue(record2.get("decimal") instanceof BigDecimal);
        Assertions.assertEquals(new BigDecimal(9876543210.0123456789), record2.get("decimal"));
        Assertions.assertEquals("Miner", record2.get("string"));
      }
    });
  }

  @Test
  public void testListPropertiesInDocument() {
    FileUtils.deleteRecursively(new File(DB_PATH));

    final BinarySerializer serializer = new BinarySerializer();

    new DatabaseFactory(DB_PATH, PaginatedFile.MODE.READ_WRITE).execute(new DatabaseFactory.POperation() {
      @Override
      public void execute(Database database) {
        database.getSchema().createDocumentType("Test");
        database.commit();

        List<Boolean> listOfBooleans = new ArrayList<>();
        listOfBooleans.add(true);
        listOfBooleans.add(false);

        List<Integer> listOfIntegers = new ArrayList<>();
        for (int i = 0; i < 100; ++i)
          listOfIntegers.add(i);

        List<Long> listOfLongs = new ArrayList<>();
        for (int i = 0; i < 100; ++i)
          listOfLongs.add((long) i);

        List<Short> listOfShorts = new ArrayList<>();
        for (int i = 0; i < 100; ++i)
          listOfShorts.add((short) i);

        List<Float> listOfFloats = new ArrayList<>();
        for (int i = 0; i < 100; ++i)
          listOfFloats.add(((float) i) + 0.123f);

        List<Double> listOfDoubles = new ArrayList<>();
        for (int i = 0; i < 100; ++i)
          listOfDoubles.add(((double) i) + 0.123f);

        List<String> listOfStrings = new ArrayList<>();
        for (int i = 0; i < 100; ++i)
          listOfStrings.add("" + i);

        List<Object> listOfMixed = new ArrayList<>();
        listOfMixed.add((int) 0);
        listOfMixed.add((long) 1);
        listOfMixed.add((short) 2);
        listOfMixed.add("3");

        database.begin();
        final ModifiableDocument v = database.newDocument("Test");
        v.set("listOfBooleans", listOfBooleans);
        v.set("listOfIntegers", listOfIntegers);
        v.set("listOfLongs", listOfLongs);
        v.set("listOfShorts", listOfShorts);
        v.set("listOfFloats", listOfFloats);
        v.set("listOfDoubles", listOfDoubles);
        v.set("listOfStrings", listOfStrings);
        v.set("listOfMixed", listOfMixed);

        final Binary buffer = serializer.serialize(database, v, -1);

        final ByteBuffer buffer2 = ByteBuffer.allocate(Bucket.DEF_PAGE_SIZE);
        buffer2.put(buffer.toByteArray());
        buffer2.flip();

        Binary buffer3 = new Binary(buffer2);
        buffer3.getByte(); // SKIP RECORD TYPE
        Map<String, Object> record2 = serializer.deserializeProperties(database, buffer3);

        Assertions.assertIterableEquals(listOfBooleans, (Iterable<?>) record2.get("listOfBooleans"));
        Assertions.assertIterableEquals(listOfIntegers, (Iterable<?>) record2.get("listOfIntegers"));
        Assertions.assertIterableEquals(listOfLongs, (Iterable<?>) record2.get("listOfLongs"));
        Assertions.assertIterableEquals(listOfShorts, (Iterable<?>) record2.get("listOfShorts"));
        Assertions.assertIterableEquals(listOfFloats, (Iterable<?>) record2.get("listOfFloats"));
        Assertions.assertIterableEquals(listOfDoubles, (Iterable<?>) record2.get("listOfDoubles"));
        Assertions.assertIterableEquals(listOfStrings, (Iterable<?>) record2.get("listOfStrings"));
        Assertions.assertIterableEquals(listOfMixed, (Iterable<?>) record2.get("listOfMixed"));
      }
    });
  }
}