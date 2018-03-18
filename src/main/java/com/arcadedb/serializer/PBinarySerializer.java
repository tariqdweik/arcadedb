package com.arcadedb.serializer;

import com.arcadedb.database.*;
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

  public PBinary serialize(final PDatabase database, final PRecord record) {
    final PBinary header = new PBinary();
    final PBinary content = new PBinary();

    header.putByte(record.getRecordType()); // RECORD TYPE

    final Set<String> propertyNames = record.getPropertyNames();
    header.putNumber(propertyNames.size());

    final int startHeaderFieldTable = header.position();

    for (String p : propertyNames) {
      // WRITE PROPERTY ID FROM THE DICTIONARY
      // TODO: USE UNSIGNED SHORT
      header.putShort((short) database.getSchema().getDictionary().getIdByName(p, true));

      final Object value = record.get(p);

      final int startContentPosition =
          content.position() + (startHeaderFieldTable + propertyNames.size() * (PBinary.SHORT_SERIALIZED_SIZE
              + PBinary.SHORT_SERIALIZED_SIZE));

      final byte type = PBinaryTypes.getTypeFromValue(value);
      content.putByte(type);

      serializeValue(content, type, value);

      // WRITE PROPERTY CONTENT POSITION
      header.putShort((short) startContentPosition);
    }

    content.flip();
    header.append(content);
    header.flip();
    return header;
  }

  public Set<String> getPropertyNames(final PDatabase database, final PBinary buffer) {
    buffer.reset();
    final byte recordType = buffer.getByte();
    final int properties = (int) buffer.getNumber();
    final Set<String> result = new LinkedHashSet<String>(properties);

    for (int i = 0; i < properties; ++i) {
      final int nameId = (int) buffer.getShort();
      final int contentPosition = (int) buffer.getShort();
      final String name = database.getSchema().getDictionary().getNameById(nameId);
      result.add(name);
    }

    return result;
  }

  public Map<String, Object> deserializeFields(final PDatabase database, final PBinary buffer, final String... fieldNames) {
    buffer.reset();
    final byte recordType = buffer.getByte();
    final int properties = (int) buffer.getNumber();

    final Map<String, Object> values = new LinkedHashMap<String, Object>(properties);

    int lastHeaderPosition;

    final int[] fieldIds = new int[fieldNames.length];
    for (int i = 0; i < fieldNames.length; ++i)
      fieldIds[i] = database.getSchema().getDictionary().getIdByName(fieldNames[i], false);

    for (int i = 0; i < properties; ++i) {
      final int nameId = (int) buffer.getShort();
      final int contentPosition = (int) buffer.getShort();

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

      final Object value;

      buffer.position(contentPosition);

      value = deserializeValue(database, buffer, buffer.getByte());

      values.put(name, value);

      buffer.position(lastHeaderPosition);
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
    case PBinaryTypes.TYPE_RID:
      final PRID rid = ((PIdentifiable) value).getIdentity();
      content.putNumber(rid.getBucketId());
      content.putNumber(rid.getPosition());
      break;
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
    case PBinaryTypes.TYPE_RID:
      value = new PRID(database, (int) content.getNumber(), content.getNumber());
      break;
    default:
      PLogManager.instance().info(this, "Error on deserializing value of type " + type);
      value = null;
    }
    return value;
  }
}
