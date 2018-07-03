/*
 * Copyright (c) 2018 - Arcade Analytics LTD (https://arcadeanalytics.com)
 */

package com.arcadedb.serializer;

import com.arcadedb.database.*;
import com.arcadedb.engine.Dictionary;
import com.arcadedb.exception.SerializationException;
import com.arcadedb.graph.*;
import com.arcadedb.utility.LogManager;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.*;

/**
 * Default serializer implementation.
 * <p>
 * TODO: check on storing all the property ids at the beginning of the buffer, so to partial deserialize values is much more
 * <p>
 * TODO: efficient, because it doesn't need to unmarshall all the values first.
 */
public class BinarySerializer {
  private BinaryComparator comparator = new BinaryComparator(this);

  public BinaryComparator getComparator() {
    return comparator;
  }

  public Binary serialize(final Database database, final Record record, final int bucketId) {
    switch (record.getRecordType()) {
    case Document.RECORD_TYPE:
      return serializeDocument(database, (ModifiableDocument) record);
    case Vertex.RECORD_TYPE:
      return serializeVertex(database, (ModifiableVertex) record, bucketId);
    case Edge.RECORD_TYPE:
      return serializeEdge(database, (ModifiableEdge) record, bucketId);
    case EdgeChunk.RECORD_TYPE:
      return serializeEdgeContainer(database, (EdgeChunk) record);
    default:
      throw new IllegalArgumentException("Cannot serialize a record of type=" + record.getRecordType());
    }
  }

  public Binary serializeDocument(final Database database, final Document document) {
    final Binary header = ((EmbeddedDatabase) database).getContext().temporaryBuffer1;
    header.clear();
    header.putByte(document.getRecordType()); // RECORD TYPE
    return serializeProperties(database, document, header);
  }

  public Binary serializeVertex(final Database database, final ModifiableVertex vertex, final int bucketId) {
    Binary header = vertex.getBuffer();

    final boolean serializeProperties;
    if (header == null) {
      header = ((EmbeddedDatabase) database).getContext().temporaryBuffer1;
      header.clear();
      header.putByte(vertex.getRecordType()); // RECORD TYPE
      serializeProperties = true;
    } else {
      // COPY THE CONTENT (THE BUFFER IS IMMUTABLE)
      header = header.copy();
      header.position(Binary.BYTE_SERIALIZED_SIZE);
      serializeProperties = false;
    }

    // WRITE OUT AND IN EDGES POINTER FIRST, THEN SERIALIZE THE VERTEX PROPERTIES (AS A DOCUMENT)
    final RID outEdges = vertex.getOutEdgesHeadChunk();
    if (outEdges != null) {
      header.putInt(outEdges.getBucketId());
      header.putLong(outEdges.getPosition());
    } else {
      header.putInt(-1);
      header.putLong(-1);
    }

    final RID inEdges = vertex.getInEdgesHeadChunk();
    if (inEdges != null) {
      header.putInt(inEdges.getBucketId());
      header.putLong(inEdges.getPosition());
    } else {
      header.putInt(-1);
      header.putLong(-1);
    }

    if (serializeProperties)
      return serializeProperties(database, vertex, header);

    return header;
  }

  public Binary serializeEdge(final Database database, final ModifiableEdge edge, final int bucketId) {
    Binary header = edge.getBuffer();

    final boolean serializeProperties;
    if (header == null) {
      header = ((EmbeddedDatabase) database).getContext().temporaryBuffer1;
      header.clear();
      header.putByte(edge.getRecordType()); // RECORD TYPE
      serializeProperties = true;
    } else {
      // COPY THE CONTENT (THE BUFFER IS IMMUTABLE)
      header = header.copy();
      header.position(Binary.BYTE_SERIALIZED_SIZE);
      serializeProperties = false;
    }

    // WRITE OUT AND IN EDGES POINTER FIRST, THEN SERIALIZE THE VERTEX PROPERTIES (AS A DOCUMENT)
    final RID outEdges = edge.getOut();
    header.putInt(outEdges.getBucketId());
    header.putLong(outEdges.getPosition());

    final RID inEdges = edge.getIn();
    header.putInt(inEdges.getBucketId());
    header.putLong(inEdges.getPosition());

    if (serializeProperties)
      return serializeProperties(database, edge, header);

    return header;
  }

  public Binary serializeEdgeContainer(final Database database, final EdgeChunk record) {
    return record.getContent();
  }

  public Set<String> getPropertyNames(final Database database, final Binary buffer) {
    final int headerSize = buffer.getInt();
    final int properties = (int) buffer.getNumber();
    final Set<String> result = new LinkedHashSet<String>(properties);

    for (int i = 0; i < properties; ++i) {
      final int nameId = (int) buffer.getNumber();
      final long contentPosition = buffer.getNumber();
      final String name = database.getSchema().getDictionary().getNameById(nameId);
      result.add(name);
    }

    return result;
  }

  public Map<String, Object> deserializeProperties(final Database database, final Binary buffer, final String... fieldNames) {
    final int headerSize = buffer.getInt();
    final int properties = (int) buffer.getNumber();

    if (properties < 0)
      throw new SerializationException("Error on deserialize record. It may be corrupted (properties=" + properties + ")");

    final Map<String, Object> values = new LinkedHashMap<String, Object>(properties);

    int lastHeaderPosition;

    final int[] fieldIds = new int[fieldNames.length];
    for (int i = 0; i < fieldNames.length; ++i)
      fieldIds[i] = database.getSchema().getDictionary().getIdByName(fieldNames[i], false);

    for (int i = 0; i < properties; ++i) {
      final int nameId = (int) buffer.getNumber();
      final int contentPosition = (int) buffer.getNumber();

      lastHeaderPosition = buffer.position();

      if (fieldIds.length > 0) {
        boolean found = false;
        // FILTER BY FIELD
        for (int f : fieldIds)
          if (f == nameId) {
            found = true;
            break;
          }

        if (!found)
          continue;
      }

      final String name = database.getSchema().getDictionary().getNameById(nameId);

      buffer.position(headerSize + contentPosition);

      final Object value = deserializeValue(database, buffer, buffer.getByte());

      values.put(name, value);

      buffer.position(lastHeaderPosition);

      if (fieldIds.length > 0 && values.size() >= fieldIds.length)
        // ALL REQUESTED PROPERTIES ALREADY FOUND
        break;
    }

    return values;
  }

  public void serializeValue(final Binary content, final byte type, final Object value) {
    switch (type) {
    case BinaryTypes.TYPE_NULL:
      break;
    case BinaryTypes.TYPE_STRING:
      content.putString((String) value);
      break;
    case BinaryTypes.TYPE_BYTE:
      content.putByte((Byte) value);
      break;
    case BinaryTypes.TYPE_BOOLEAN:
      content.putByte((byte) ((Boolean) value ? 1 : 0));
      break;
    case BinaryTypes.TYPE_SHORT:
      content.putNumber(((Number) value).shortValue());
      break;
    case BinaryTypes.TYPE_INT:
      content.putNumber(((Number) value).intValue());
      break;
    case BinaryTypes.TYPE_LONG:
      content.putNumber(((Number) value).longValue());
      break;
    case BinaryTypes.TYPE_FLOAT:
      final int fg = Float.floatToIntBits(((Number) value).floatValue());
      content.putNumber(fg);
      break;
    case BinaryTypes.TYPE_DOUBLE:
      final long dg = Double.doubleToLongBits(((Number) value).doubleValue());
      content.putNumber(dg);
      break;
    case BinaryTypes.TYPE_DATE:
      content.putNumber(((Date) value).getTime());
      break;
    case BinaryTypes.TYPE_DATETIME:
      content.putNumber(((Date) value).getTime());
      break;
    case BinaryTypes.TYPE_DECIMAL:
      content.putNumber(((BigDecimal) value).scale());
      content.putBytes(((BigDecimal) value).unscaledValue().toByteArray());
      break;
    case BinaryTypes.TYPE_COMPRESSED_RID: {
      final RID rid = ((Identifiable) value).getIdentity();
      content.putNumber(rid.getBucketId());
      content.putNumber(rid.getPosition());
      break;
    }
    case BinaryTypes.TYPE_RID: {
      if (value == null) {
        content.putInt(-1);
        content.putLong(-1);
      } else {
        final RID rid = ((Identifiable) value).getIdentity();
        content.putInt(rid.getBucketId());
        content.putLong(rid.getPosition());
      }
      break;
    }
    default:
      LogManager.instance().info(this, "Error on serializing value '" + value + "', type not supported");
    }
  }

  public Object deserializeValue(final Database database, final Binary content, final byte type) {
    Object value;
    switch (type) {
    case BinaryTypes.TYPE_NULL:
      value = null;
      break;
    case BinaryTypes.TYPE_STRING:
      value = content.getString();
      break;
    case BinaryTypes.TYPE_BYTE:
      value = content.getByte();
      break;
    case BinaryTypes.TYPE_BOOLEAN:
      value = content.getByte() == 1;
      break;
    case BinaryTypes.TYPE_SHORT:
      value = (short) content.getNumber();
      break;
    case BinaryTypes.TYPE_INT:
      value = (int) content.getNumber();
      break;
    case BinaryTypes.TYPE_LONG:
      value = content.getNumber();
      break;
    case BinaryTypes.TYPE_FLOAT:
      value = Float.intBitsToFloat((int) content.getNumber());
      break;
    case BinaryTypes.TYPE_DOUBLE:
      value = Double.longBitsToDouble(content.getNumber());
      break;
    case BinaryTypes.TYPE_DATE:
      value = new Date(content.getNumber());
      break;
    case BinaryTypes.TYPE_DATETIME:
      value = new Date(content.getNumber());
      break;
    case BinaryTypes.TYPE_DECIMAL:
      final int scale = (int) content.getNumber();
      final byte[] unscaledValue = content.getBytes();
      value = new BigDecimal(new BigInteger(unscaledValue), scale);
      break;
    case BinaryTypes.TYPE_COMPRESSED_RID:
      value = new RID(database, (int) content.getNumber(), content.getNumber());
      break;
    case BinaryTypes.TYPE_RID:
      value = new RID(database, content.getInt(), content.getLong());
      break;
    default:
      LogManager.instance().info(this, "Error on deserializing value of type " + type);
      value = null;
    }
    return value;
  }

  public Binary serializeProperties(final Database database, final Document record, final Binary header) {
    final int headerSizePosition = header.position();
    header.putInt(0); // TEMPORARY PLACEHOLDER FOR HEADER SIZE

    final Set<String> propertyNames = record.getPropertyNames();
    header.putNumber(propertyNames.size());

    final Binary content = ((EmbeddedDatabase) database).getContext().temporaryBuffer2;
    content.clear();

    final Dictionary dictionary = database.getSchema().getDictionary();

    for (String p : propertyNames) {
      // WRITE PROPERTY ID FROM THE DICTIONARY
      // TODO: USE UNSIGNED SHORT
      header.putNumber(dictionary.getIdByName(p, true));

      final Object value = record.get(p);

      final int startContentPosition = content.position();

      final byte type = BinaryTypes.getTypeFromValue(value);
      content.putByte(type);

      serializeValue(content, type, value);

      // WRITE PROPERTY CONTENT POSITION
      header.putNumber(startContentPosition);
    }

    content.flip();

    final int headerSize = header.position();

    header.append(content);

    // UPDATE HEADER SIZE
    header.putInt(headerSizePosition, headerSize);

    header.position(header.size());
    header.flip();
    return header;
  }
}
