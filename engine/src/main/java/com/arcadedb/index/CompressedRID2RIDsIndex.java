/*
 * Copyright (c) 2018 - Arcade Analytics LTD (https://arcadeanalytics.com)
 */

package com.arcadedb.index;

import com.arcadedb.database.Binary;
import com.arcadedb.database.Database;
import com.arcadedb.database.RID;
import com.arcadedb.serializer.BinarySerializer;
import com.arcadedb.serializer.BinaryTypes;
import com.arcadedb.utility.Pair;

import java.util.ArrayList;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Map like optimized to avoid stressing the GC by using mechanical sympathy technique + compression of key and values.
 * This class is synchronized. The key is a RID and values are pairs of RIDs. This Map implementation doesn't support the overwrite of a
 * value. Values cannot be null.
 * <p>
 * This index is used for invert incoming edge creation.
 * <p>
 * A Binary object is used to store the hash table (the first part of it) and then keys and values. The RID key is serialized compressed in
 * the position pointed by the hash table, then a fixed-size integer containing the next entry (with the same hash) and after that the
 * compressed RIDs pair (edge+vertex). Another slot is kept to point to the previous entry. The hash table always points to the last element
 * with a linked list in the only direction of the previous.
 *
 * @TODO support up to 4GB by using unsigned int
 */
public class CompressedRID2RIDsIndex {
  protected final Database         database;
  protected final BinarySerializer serializer;
  protected final AtomicInteger    resetCounter = new AtomicInteger();
  protected final int keys;

  protected Binary chunk;
  protected int    totalEntries   = 0;
  protected int    totalUsedSlots = 0;

  public class EntryIterator {
    private int posInHashTable = 0;
    private int posInChunk     = 0;
    private int nextEntryPos   = 0;
    private int nextKeyPos;

    private RID nextKey;
    private RID nextEdgeRID;
    private RID nextVertexRID;

    public boolean hasNext() {
      if (nextEdgeRID != null)
        return true;

      if (totalUsedSlots == 0)
        return false;

      if (nextEntryPos > 0) {
        // SAME KEY NEXT ENTRY
        chunk.position(nextEntryPos);

        nextEdgeRID = (RID) serializer.deserializeValue(database, chunk, BinaryTypes.TYPE_COMPRESSED_RID);
        nextVertexRID = (RID) serializer.deserializeValue(database, chunk, BinaryTypes.TYPE_COMPRESSED_RID);
        nextEntryPos = chunk.getInt();
        return true;
      }

      if (nextKeyPos > 0) {
        // NEXT KEY ON SAME POSITION IN HASHTABLE
        chunk.position(nextKeyPos);

        nextKey = (RID) serializer.deserializeValue(database, chunk, BinaryTypes.TYPE_COMPRESSED_RID);
        nextKeyPos = chunk.getInt();
        nextEntryPos = chunk.getInt();
        nextEdgeRID = (RID) serializer.deserializeValue(database, chunk, BinaryTypes.TYPE_COMPRESSED_RID);
        nextVertexRID = (RID) serializer.deserializeValue(database, chunk, BinaryTypes.TYPE_COMPRESSED_RID);
        return true;
      }

      // NEXT POSITION IN HASHTABLE
      for (; posInHashTable < keys; ++posInHashTable) {
        posInChunk = chunk.getInt(posInHashTable * Binary.INT_SERIALIZED_SIZE);
        if (posInChunk > 0) {
          chunk.position(posInChunk);

          // READ -> RID|INT|INT|RID|RID

          nextKey = (RID) serializer.deserializeValue(database, chunk, BinaryTypes.TYPE_COMPRESSED_RID);
          nextKeyPos = chunk.getInt();
          nextEntryPos = chunk.getInt();
          nextEdgeRID = (RID) serializer.deserializeValue(database, chunk, BinaryTypes.TYPE_COMPRESSED_RID);
          nextVertexRID = (RID) serializer.deserializeValue(database, chunk, BinaryTypes.TYPE_COMPRESSED_RID);
          ++posInHashTable;
          return true;
        }
      }

      return false;
    }

    public RID getKey() {
      if (!hasNext())
        throw new NoSuchElementException();
      return nextKey;
    }

    public RID getEdge() {
      if (!hasNext())
        throw new NoSuchElementException();
      return nextEdgeRID;
    }

    public RID getVertex() {
      if (!hasNext())
        throw new NoSuchElementException();
      return nextVertexRID;
    }

    public void moveNext() {
      nextEdgeRID = null;
    }
  }

  public CompressedRID2RIDsIndex(final Database database, final int expectedSize) {
    this.database = database;
    this.keys = expectedSize;
    this.serializer = new BinarySerializer();
    reset();
  }

  public CompressedRID2RIDsIndex(final Database database, final Binary buffer) {
    this.database = database;
    this.keys = buffer.size();
    this.serializer = new BinarySerializer();
    this.chunk = buffer;
  }

  public int size() {
    return totalEntries;
  }

  public boolean isEmpty() {
    return totalEntries == 0;
  }

  public boolean containsKey(final RID key) {
    if (key == null)
      throw new IllegalArgumentException("Key is null");

    return get(key) != null;
  }

  public List<Pair<RID, RID>> get(final RID key) {
    if (key == null)
      throw new IllegalArgumentException("Key is null");

    final int hash = Math.abs(key.hashCode()) % keys;

    synchronized (this) {
      final int pos = chunk.getInt(hash * Binary.INT_SERIALIZED_SIZE);
      if (pos == 0)
        return null;

      // SLOT OCCUPIED, CHECK FOR THE KEY
      chunk.position(pos);
      while (true) {
        Object slotKey = serializer.deserializeValue(database, chunk, BinaryTypes.TYPE_COMPRESSED_RID);

        if (slotKey.equals(key)) {
          // FOUND KEY, COLLECT ALL THE VALUE IN THE LINKED LIST
          final List<Pair<RID, RID>> list = new ArrayList<>();

          chunk.position(chunk.position() + Binary.INT_SERIALIZED_SIZE);
          int nextEntryPos = chunk.getInt();

          RID edgeRid = (RID) serializer.deserializeValue(database, chunk, BinaryTypes.TYPE_COMPRESSED_RID);
          RID vertexRid = (RID) serializer.deserializeValue(database, chunk, BinaryTypes.TYPE_COMPRESSED_RID);
          list.add(new Pair<>(edgeRid, vertexRid));

          while (nextEntryPos > 0) {
            nextEntryPos = chunk.getInt(nextEntryPos);

            edgeRid = (RID) serializer.deserializeValue(database, chunk, BinaryTypes.TYPE_COMPRESSED_RID);
            vertexRid = (RID) serializer.deserializeValue(database, chunk, BinaryTypes.TYPE_COMPRESSED_RID);

            list.add(new Pair<>(edgeRid, vertexRid));
          }

          return list;
        }

        final int nextPos = chunk.getInt();
        if (nextPos <= 0)
          break;

        chunk.position(nextPos);
      }
    }

    return null;
  }

  public void put(final RID key, final RID edgeRID, final RID vertexRID) {
    if (key == null)
      throw new IllegalArgumentException("Key is null");

    if (edgeRID == null)
      throw new IllegalArgumentException("Value is null");

    final int hash = Math.abs(key.hashCode()) % keys;

    synchronized (this) {
      final int pos = chunk.getInt(hash * Binary.INT_SERIALIZED_SIZE);
      if (pos == 0) {
        // NEW KEY
        chunk.position(chunk.size());
        chunk.putInt(hash * Binary.INT_SERIALIZED_SIZE, chunk.position());

        // WRITE -> RID|INT|INT|RID|RID

        // WRITE THE KEY FIRST
        serializer.serializeValue(chunk, BinaryTypes.TYPE_COMPRESSED_RID, key);

        // LEAVE AN INT AS EMPTY SLOT FOR THE NEXT KEY
        chunk.putInt(0);

        // LEAVE AN INT AS EMPTY SLOT FOR THE PREVIOUS ELEMENT
        chunk.putInt(0);

        // WRITE THE VALUE
        serializer.serializeValue(chunk, BinaryTypes.TYPE_COMPRESSED_RID, edgeRID);
        serializer.serializeValue(chunk, BinaryTypes.TYPE_COMPRESSED_RID, vertexRID);

        ++totalUsedSlots;

      } else {
        // SLOT OCCUPIED, CHECK FOR THE KEY
        chunk.position(pos);
        int lastNextPos = 0;
        while (true) {
          final RID slotKey = (RID) serializer.deserializeValue(database, chunk, BinaryTypes.TYPE_COMPRESSED_RID);

          if (slotKey.equals(key)) {
            // FOUND THE KEY, GET PREVIOUS ITEM
            final int previousEntryOffset = chunk.position() + Binary.INT_SERIALIZED_SIZE; // SKIP NEXT KEY
            final int previousEntryPos = chunk.getInt(previousEntryOffset);

            // APPEND THE NEW ENTRY
            chunk.position(chunk.size());

            final int newEntryPosition = chunk.position();

            // WRITE -> RID|RID|INT

            serializer.serializeValue(chunk, BinaryTypes.TYPE_COMPRESSED_RID, edgeRID);
            serializer.serializeValue(chunk, BinaryTypes.TYPE_COMPRESSED_RID, vertexRID);

            if (previousEntryPos > 0)
              // THIS IS THE 3RD OR MAJOR ENTRY. APPEND THE POSITION OF THE PREVIOUS ENTRY
              chunk.putInt(previousEntryPos);
            else
              chunk.putInt(0);

            chunk.putInt(previousEntryOffset, newEntryPosition);
            ++totalEntries;
            return;
          }

          lastNextPos = chunk.position();

          final int nextPos = chunk.getInt();
          if (nextPos <= 0)
            break;

          chunk.position(nextPos);
        }

        // APPEND TO THE END
        chunk.position(chunk.size());
        final int entryPosition = chunk.position();

        // WRITE THE KEY FIRST
        serializer.serializeValue(chunk, BinaryTypes.TYPE_COMPRESSED_RID, key);

        // LEAVE AN INT AS EMPTY SLOT FOR THE NEXT KEY
        chunk.putInt(0);

        // LEAVE AN INT AS EMPTY SLOT FOR THE PREVIOUS ELEMENT
        chunk.putInt(0);

        // WRITE THE VALUE
        serializer.serializeValue(chunk, BinaryTypes.TYPE_COMPRESSED_RID, edgeRID);
        serializer.serializeValue(chunk, BinaryTypes.TYPE_COMPRESSED_RID, vertexRID);

        // WRITE THIS ENTRY POSITION TO THE PREVIOUS NEXT POSITION FIELD
        chunk.putInt(lastNextPos, entryPosition);
      }

      ++totalEntries;
    }
  }

  public EntryIterator entryIterator() {
    return new EntryIterator();
  }

  public int getChunkSize() {
    return chunk.size();
  }

  public int getChunkAllocated() {
    return chunk.getAllocatedSize();
  }

  public int getTotalUsedSlots() {
    return totalUsedSlots;
  }

  public int getResetCounter() {
    return resetCounter.get();
  }

  public Binary reset() {
    synchronized (this) {
      final Binary oldChunk = chunk;
      this.chunk = new Binary(keys * 5);
      this.chunk.setAllocationChunkSize(keys);
      this.chunk.fill((byte) 0, keys * Binary.INT_SERIALIZED_SIZE);
      totalEntries = 0;
      totalUsedSlots = 0;
      resetCounter.incrementAndGet();
      return oldChunk;
    }
  }
}
