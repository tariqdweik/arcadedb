package com.arcadedb.serializer;

import com.arcadedb.database.*;
import com.arcadedb.graph.*;
import com.arcadedb.utility.PLogManager;

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
public class PBinarySerializer {
  private PBinaryComparator comparator = new PBinaryComparator(this);

  public PBinaryComparator getComparator() {
    return comparator;
  }

  public PBinary serialize(final PDatabase database, final PRecord record, final int bucketId) {
    switch (record.getRecordType()) {
    case PDocument.RECORD_TYPE:
      return serializeDocument(database, (PModifiableDocument) record);
    case PVertex.RECORD_TYPE:
      return serializeVertex(database, (PModifiableVertex) record, bucketId);
    case PEdge.RECORD_TYPE:
      return serializeEdge(database, (PModifiableEdge) record, bucketId);
    case PEdgeChunk.RECORD_TYPE:
      return serializeEdgeContainer(database, (PEdgeChunk) record);
    default:
      throw new IllegalArgumentException("Cannot serialize a record of type=" + record.getRecordType());
    }
  }

  public PBinary serializeDocument(final PDatabase database, final PDocument document) {
    final PBinary header = new PBinary(64);
    header.putByte(document.getRecordType()); // RECORD TYPE
    return serializeProperties(database, document, header);
  }

  public PBinary serializeVertex(final PDatabase database, final PModifiableVertex vertex, final int bucketId) {
    vertex.onSerialize(bucketId);

    final PBinary header = new PBinary(64);
    header.putByte(vertex.getRecordType()); // RECORD TYPE

    // WRITE OUT AND IN EDGES POINTER FIRST, THEN SERIALIZE THE VERTEX PROPERTIES (AS A DOCUMENT)
    final PRID outEdges = vertex.getOutEdgesHeadChunk();
    header.putInt(outEdges.getBucketId());
    header.putLong(outEdges.getPosition());

    final PRID inEdges = vertex.getInEdgesHeadChunk();
    header.putInt(inEdges.getBucketId());
    header.putLong(inEdges.getPosition());

    return serializeProperties(database, vertex, header);
  }

  public PBinary serializeEdge(final PDatabase database, final PModifiableEdge edge, final int bucketId) {
    edge.onSerialize(bucketId);

    final PBinary header = new PBinary(64);
    header.putByte(edge.getRecordType()); // RECORD TYPE

    // WRITE OUT AND IN EDGES POINTER FIRST, THEN SERIALIZE THE VERTEX PROPERTIES (AS A DOCUMENT)
    final PRID outEdges = edge.getOut();
    header.putInt(outEdges.getBucketId());
    header.putLong(outEdges.getPosition());

    final PRID inEdges = edge.getIn();
    header.putInt(inEdges.getBucketId());
    header.putLong(inEdges.getPosition());

    return serializeProperties(database, edge, header);
  }

  public PBinary serializeEdgeContainer(final PDatabase database, final PEdgeChunk record) {
    return record.getContent();
  }

  public Set<String> getPropertyNames(final PDatabase database, final PBinary buffer) {
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

  public Map<String, Object> deserializeProperties(final PDatabase database, final PBinary buffer, final String... fieldNames) {
    final int headerSize = buffer.getInt();
    final int properties = (int) buffer.getNumber();

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

  public void serializeValue(final PBinary content, final byte type, final Object value) {
    switch (type) {
    case PBinaryTypes.TYPE_NULL:
      break;
    case PBinaryTypes.TYPE_STRING:
      content.putString((String) value);
      break;
    case PBinaryTypes.TYPE_BYTE:
      content.putByte((Byte) value);
      break;
    case PBinaryTypes.TYPE_SHORT:
      content.putNumber((Short) value);
      break;
    case PBinaryTypes.TYPE_INT:
      content.putNumber((Integer) value);
      break;
    case PBinaryTypes.TYPE_LONG:
      content.putNumber((Long) value);
      break;
    case PBinaryTypes.TYPE_FLOAT:
      final int fg = Float.floatToIntBits(((Number) value).floatValue());
      content.putNumber(fg);
      break;
    case PBinaryTypes.TYPE_DOUBLE:
      final long dg = Double.doubleToLongBits(((Number) value).doubleValue());
      content.putNumber(dg);
      break;
    case PBinaryTypes.TYPE_DATE:
      content.putNumber(((Date) value).getTime());
      break;
    case PBinaryTypes.TYPE_DATETIME:
      content.putNumber(((Date) value).getTime());
      break;
    case PBinaryTypes.TYPE_DECIMAL:
      content.putNumber(((BigDecimal) value).scale());
      content.putBytes(((BigDecimal) value).unscaledValue().toByteArray());
      break;
    case PBinaryTypes.TYPE_COMPRESSED_RID: {
      final PRID rid = ((PIdentifiable) value).getIdentity();
      content.putNumber(rid.getBucketId());
      content.putNumber(rid.getPosition());
      break;
    }
    case PBinaryTypes.TYPE_RID: {
      final PRID rid = ((PIdentifiable) value).getIdentity();
      content.putInt(rid.getBucketId());
      content.putLong(rid.getPosition());
      break;
    }
    default:
      PLogManager.instance().info(this, "Error on serializing value '" + value + "', type not supported");
    }
  }

  public Object deserializeValue(final PDatabase database, final PBinary content, final byte type) {
    Object value;
    switch (type) {
    case PBinaryTypes.TYPE_NULL:
      value = null;
      break;
    case PBinaryTypes.TYPE_STRING:
      value = content.getString();
      break;
    case PBinaryTypes.TYPE_BYTE:
      value = content.getByte();
      break;
    case PBinaryTypes.TYPE_SHORT:
      value = (short) content.getNumber();
      break;
    case PBinaryTypes.TYPE_INT:
      value = (int) content.getNumber();
      break;
    case PBinaryTypes.TYPE_LONG:
      value = content.getNumber();
      break;
    case PBinaryTypes.TYPE_FLOAT:
      value = Float.intBitsToFloat((int) content.getNumber());
      break;
    case PBinaryTypes.TYPE_DOUBLE:
      value = Double.longBitsToDouble(content.getNumber());
      break;
    case PBinaryTypes.TYPE_DATE:
      value = new Date(content.getNumber());
      break;
    case PBinaryTypes.TYPE_DATETIME:
      value = new Date(content.getNumber());
      break;
    case PBinaryTypes.TYPE_DECIMAL:
      final int scale = (int) content.getNumber();
      final byte[] unscaledValue = content.getBytes();
      value = new BigDecimal(new BigInteger(unscaledValue), scale);
      break;
    case PBinaryTypes.TYPE_COMPRESSED_RID:
      value = new PRID(database, (int) content.getNumber(), content.getNumber());
      break;
    case PBinaryTypes.TYPE_RID:
      value = new PRID(database, content.getInt(), content.getLong());
      break;
    default:
      PLogManager.instance().info(this, "Error on deserializing value of type " + type);
      value = null;
    }
    return value;
  }

  public PBinary serializeProperties(final PDatabase database, final PDocument record, final PBinary header) {
    final int headerSizePosition = header.position();
    header.putInt(0); // TEMPORARY PLACEHOLDER FOR HEADER SIZE

    final Set<String> propertyNames = record.getPropertyNames();
    header.putNumber(propertyNames.size());

    final PBinary content = new PBinary();

    for (String p : propertyNames) {
      // WRITE PROPERTY ID FROM THE DICTIONARY
      // TODO: USE UNSIGNED SHORT
      header.putNumber(database.getSchema().getDictionary().getIdByName(p, true));

      final Object value = record.get(p);

      final int startContentPosition = content.position();

      final byte type = PBinaryTypes.getTypeFromValue(value);
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
