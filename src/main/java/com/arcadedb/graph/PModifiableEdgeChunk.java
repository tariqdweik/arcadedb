package com.arcadedb.graph;

import com.arcadedb.database.*;
import com.arcadedb.serializer.PBinaryTypes;

import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

public class PModifiableEdgeChunk extends PBaseRecord implements PEdgeChunk, PRecordInternal {
  public static final  byte RECORD_TYPE            = 3;
  public static final  int  CONTENT_START_POSITION =
      PBinary.BYTE_SERIALIZED_SIZE + PBinary.INT_SERIALIZED_SIZE + PBinaryTypes.getTypeSize(PBinaryTypes.TYPE_RID);
  private static final PRID NULL_RID               = new PRID(null, -1, -1);

  private int bufferSize;

  public PModifiableEdgeChunk(final PDatabase database, final PRID rid) {
    super(database, rid, null);
    this.buffer = null;
  }

  public PModifiableEdgeChunk(final PDatabase database, final PRID rid, final PBinary buffer) {
    super(database, rid, buffer);
    this.buffer = buffer;
    this.bufferSize = buffer.size();
  }

  public PModifiableEdgeChunk(final PDatabase database, final int bufferSize) {
    super(database, null, new PBinary(bufferSize));
    this.bufferSize = bufferSize;
    buffer.putByte(0, RECORD_TYPE); // USED
    buffer.putInt(PBinary.BYTE_SERIALIZED_SIZE, CONTENT_START_POSITION); // USED
    buffer.position(PBinary.BYTE_SERIALIZED_SIZE + PBinary.INT_SERIALIZED_SIZE);
    database.getSerializer().serializeValue(buffer, PBinaryTypes.TYPE_RID, NULL_RID); // NEXT
  }

  @Override
  public byte getRecordType() {
    return RECORD_TYPE;
  }

  @Override
  public PRecord modify() {
    return this;
  }

  @Override
  public boolean add(final PRID edgeRID, final PRID vertexRID) {
    final PBinary ridSerialized = new PBinary(28);
    database.getSerializer().serializeValue(ridSerialized, PBinaryTypes.TYPE_COMPRESSED_RID, edgeRID);
    database.getSerializer().serializeValue(ridSerialized, PBinaryTypes.TYPE_COMPRESSED_RID, vertexRID);

    final int used = getUsed();

    if (used + ridSerialized.size() <= bufferSize) {
      // APPEND AT THE END OF THE CURRENT CHUNK
      buffer.putByteArray(used, ridSerialized.getContent(), ridSerialized.size());

      // UPDATE USED BYTES
      buffer.putInt(PBinary.BYTE_SERIALIZED_SIZE, used + ridSerialized.size());
      // TODO save()

      return true;
    }

    // NO ROOM
    return false;
  }

  @Override
  public boolean containsEdge(final PRID rid) {
    final int used = getUsed();
    if (used == 0)
      return false;

    final int bucketId = rid.getBucketId();
    final long position = rid.getPosition();

    buffer.position(CONTENT_START_POSITION);

    while (buffer.position() < used) {
      final int currEdgeBucketId = (int) buffer.getNumber();
      final long currEdgePosition = buffer.getNumber();
      if (currEdgeBucketId == bucketId && currEdgePosition == position)
        return true;

      // SKIP VERTEX RID
      buffer.getNumber();
      buffer.getNumber();
    }

    return false;
  }

  @Override
  public boolean containsVertex(final PRID rid) {
    final int used = getUsed();
    if (used == 0)
      return false;

    final int bucketId = rid.getBucketId();
    final long position = rid.getPosition();

    buffer.position(CONTENT_START_POSITION);

    while (buffer.position() < used) {
      // SKIP EDGE RID
      buffer.getNumber();
      buffer.getNumber();

      final int currEdgeBucketId = (int) buffer.getNumber();
      final long currEdgePosition = buffer.getNumber();
      if (currEdgeBucketId == bucketId && currEdgePosition == position)
        return true;
    }

    return false;
  }

  @Override
  public long count(final Set<Integer> fileIds) {
    long total = 0;

    final int used = getUsed();
    if (used > 0) {
      buffer.position(CONTENT_START_POSITION);

      while (buffer.position() < used) {
        final int fileId = (int) buffer.getNumber();
        // SKIP EDGE RID POSITION AND VERTEX RID
        buffer.getNumber();
        buffer.getNumber();
        buffer.getNumber();

        if (fileIds != null) {
          if (fileIds.contains(fileId))
            ++total;
        } else
          ++total;
      }
    }

    return total;
  }

  @Override
  public PEdgeChunk getNext() {
    buffer.position(PBinary.BYTE_SERIALIZED_SIZE + PBinary.INT_SERIALIZED_SIZE);

    final PRID nextRID = (PRID) database.getSerializer().deserializeValue(database, buffer, PBinaryTypes.TYPE_RID); // NEXT

    if (nextRID.getBucketId() == -1 && nextRID.getPosition() == -1)
      return null;

    return (PEdgeChunk) database.lookupByRID(nextRID, true);
  }

  @Override
  public void setNext(final PEdgeChunk next) {
    final PRID nextRID = next.getIdentity();
    if (nextRID == null)
      throw new IllegalArgumentException("Next chunk is not persistent");
    buffer.position(PBinary.BYTE_SERIALIZED_SIZE + PBinary.INT_SERIALIZED_SIZE);
    database.getSerializer().serializeValue(buffer, PBinaryTypes.TYPE_RID, nextRID); // NEXT
  }

  @Override
  public PBinary getContent() {
    buffer.position(bufferSize);
    buffer.flip();
    return buffer;
  }

  @Override
  public int getUsed() {
    return buffer.getInt(PBinary.BYTE_SERIALIZED_SIZE);
  }

  @Override
  public PRID getEdge(final AtomicInteger currentPosition) {
    buffer.position(currentPosition.get());
    final PRID next = (PRID) database.getSerializer().deserializeValue(database, buffer, PBinaryTypes.TYPE_COMPRESSED_RID); // NEXT
    currentPosition.set(buffer.position());
    return next;
  }

  @Override
  public PRID getVertex(final AtomicInteger currentPosition) {
    buffer.position(currentPosition.get());
    final PRID next = (PRID) database.getSerializer().deserializeValue(database, buffer, PBinaryTypes.TYPE_COMPRESSED_RID); // NEXT
    currentPosition.set(buffer.position());
    return next;
  }

  @Override
  public int getRecordSize() {
    return buffer.size();
  }

  @Override
  public void setIdentity(final PRID rid) {
    this.rid = rid;
  }
}
