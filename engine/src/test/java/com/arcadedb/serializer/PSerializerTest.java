/*
 * Copyright (c) 2018 - Arcade Analytics LTD (https://arcadeanalytics.com)
 */

package com.arcadedb.serializer;

import com.arcadedb.database.*;
import com.arcadedb.engine.Bucket;
import com.arcadedb.engine.PaginatedFile;
import com.arcadedb.utility.FileUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.nio.ByteBuffer;
import java.util.Map;

public class PSerializerTest {

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
  public void testPropertiesInDocument() {
    FileUtils.deleteRecursively(new File(DB_PATH));

    final BinarySerializer serializer = new BinarySerializer();

    new DatabaseFactory(DB_PATH, PaginatedFile.MODE.READ_WRITE).execute(new DatabaseFactory.POperation() {
      @Override
      public void execute(Database database) {
        database.getSchema().createDocumentType("Test");
        database.commit();

        database.begin();

        final ModifiableDocument v = database.newDocument("Test");
        v.set("id", 0);
        v.set("idLong", Long.MAX_VALUE);
        v.set("name", "Jay");
        v.set("surname", "Miner");

        final Binary buffer = serializer.serialize(database, v, -1);

        final ByteBuffer buffer2 = ByteBuffer.allocate(Bucket.DEF_PAGE_SIZE);
        buffer2.put(buffer.toByteArray());
        buffer2.flip();

        Binary buffer3 = new Binary(buffer2);
        buffer3.getByte(); // SKIP RECORD TYPE
        Map<String, Object> record2 = serializer.deserializeProperties((EmbeddedDatabase) database, buffer3);

        Assertions.assertEquals(4, record2.size());
        Assertions.assertEquals(0, record2.get("id"));
        Assertions.assertEquals(Long.MAX_VALUE, record2.get("idLong"));
        Assertions.assertEquals("Jay", record2.get("name"), "Jay");
        Assertions.assertEquals("Miner", record2.get("surname"));
      }
    });
  }
}