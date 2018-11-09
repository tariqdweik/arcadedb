/*
 * Copyright (c) 2018 - Arcade Analytics LTD (https://arcadeanalytics.com)
 */
package com.arcadedb.server.ha.message;

import com.arcadedb.database.*;
import com.arcadedb.engine.CompressionFactory;
import com.arcadedb.engine.WALFile;
import com.arcadedb.exception.NeedRetryException;
import com.arcadedb.exception.TransactionException;
import com.arcadedb.log.LogManager;
import com.arcadedb.serializer.BinarySerializer;
import com.arcadedb.serializer.BinaryTypes;
import com.arcadedb.server.ha.HAServer;
import com.arcadedb.server.ha.ReplicationException;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.logging.Level;

/**
 * Forward a transaction to the Leader server to be executed. Apart the TX content (like with TxRequest), unique keys list is
 * needed to assure the index unique constraint.
 */
public class TxForwardRequest extends TxRequestAbstract {
  private int    uniqueKeysUncompressedLength;
  private Binary uniqueKeysBuffer;

  public TxForwardRequest() {
  }

  public TxForwardRequest(final DatabaseInternal database, final Binary bufferChanges,
      final Map<String, Map<TransactionIndexContext.ComparableKey, Set<TransactionIndexContext.IndexKey>>> keysTx) {
    super(database.getName(), bufferChanges);
    writeIndexKeysToBuffer(database, keysTx);
  }

  @Override
  public void toStream(final Binary stream) {
    super.toStream(stream);
    stream.putInt(uniqueKeysUncompressedLength);
    stream.putBytes(uniqueKeysBuffer.getContent(), uniqueKeysBuffer.size());
  }

  @Override
  public void fromStream(final Binary stream) {
    super.fromStream(stream);
    uniqueKeysUncompressedLength = stream.getInt();
    uniqueKeysBuffer = CompressionFactory.getDefault().decompress(new Binary(stream.getBytes()), uniqueKeysUncompressedLength);
  }

  @Override
  public HACommand execute(final HAServer server, final String remoteServerName, final long messageNumber) {
    final DatabaseInternal db = (DatabaseInternal) server.getServer().getDatabase(databaseName);
    if (!db.isOpen())
      throw new ReplicationException("Database '" + databaseName + "' is closed");

    try {
      final WALFile.WALTransaction walTx = readTxFromBuffer();
      final Map<String, Map<TransactionIndexContext.ComparableKey, Set<TransactionIndexContext.IndexKey>>> keysTx = readIndexKeysFromBuffer(db);

      // FORWARDED FROM A REPLICA
      db.begin();
      final TransactionContext tx = db.getTransaction();

      tx.commitFromReplica(walTx, keysTx);

    } catch (NeedRetryException | TransactionException e) {
      return new ErrorResponse(e);
    } catch (Exception e) {
      LogManager.instance().log(this, Level.SEVERE, "Error with the execution of the forwarded message %d", e, messageNumber);
      return new ErrorResponse(e);
    }

    return new TxForwardResponse();
  }

  @Override
  public String toString() {
    return "tx-forward(" + databaseName + ")";
  }

  protected void writeIndexKeysToBuffer(final DatabaseInternal database,
      final Map<String, Map<TransactionIndexContext.ComparableKey, Set<TransactionIndexContext.IndexKey>>> indexesChanges) {
    final BinarySerializer serializer = database.getSerializer();

    uniqueKeysBuffer = new Binary();

    uniqueKeysBuffer.putUnsignedNumber(indexesChanges.size());

    for (Map.Entry<String, Map<TransactionIndexContext.ComparableKey, Set<TransactionIndexContext.IndexKey>>> entry : indexesChanges.entrySet()) {
      uniqueKeysBuffer.putString(entry.getKey());
      final Map<TransactionIndexContext.ComparableKey, Set<TransactionIndexContext.IndexKey>> indexChanges = entry.getValue();

      uniqueKeysBuffer.putUnsignedNumber(indexChanges.size());

      for (Map.Entry<TransactionIndexContext.ComparableKey, Set<TransactionIndexContext.IndexKey>> keyChange : indexChanges.entrySet()) {
        final TransactionIndexContext.ComparableKey entryKey = keyChange.getKey();

        uniqueKeysBuffer.putUnsignedNumber(entryKey.values.length);
        for (int k = 0; k < entryKey.values.length; ++k) {
          final byte keyType = BinaryTypes.getTypeFromValue(entryKey.values[k]);
          uniqueKeysBuffer.putByte(keyType);
          serializer.serializeValue(uniqueKeysBuffer, keyType, entryKey.values[k]);
        }

        final Set<TransactionIndexContext.IndexKey> entryValue = keyChange.getValue();

        uniqueKeysBuffer.putUnsignedNumber(entryValue.size());

        for (TransactionIndexContext.IndexKey key : entryValue) {
          uniqueKeysBuffer.putByte((byte) (key.addOperation ? 1 : 0));
          uniqueKeysBuffer.putUnsignedNumber(key.rid.getBucketId());
          uniqueKeysBuffer.putUnsignedNumber(key.rid.getPosition());
        }
      }
    }

    uniqueKeysUncompressedLength = uniqueKeysBuffer.size();
    uniqueKeysBuffer.rewind();
    uniqueKeysBuffer = CompressionFactory.getDefault().compress(uniqueKeysBuffer);
  }

  protected Map<String, Map<TransactionIndexContext.ComparableKey, Set<TransactionIndexContext.IndexKey>>> readIndexKeysFromBuffer(
      final DatabaseInternal database) {
    final BinarySerializer serializer = database.getSerializer();

    uniqueKeysBuffer.position(0);

    final int totalIndexes = (int) uniqueKeysBuffer.getUnsignedNumber();

    final Map<String, Map<TransactionIndexContext.ComparableKey, Set<TransactionIndexContext.IndexKey>>> indexesMap = new HashMap<>(totalIndexes);

    for (int indexIdx = 0; indexIdx < totalIndexes; ++indexIdx) {
      final String indexName = uniqueKeysBuffer.getString();

      final int totalIndexEntries = (int) uniqueKeysBuffer.getUnsignedNumber();

      final Map<TransactionIndexContext.ComparableKey, Set<TransactionIndexContext.IndexKey>> indexMap = new HashMap<>(totalIndexEntries);
      indexesMap.put(indexName, indexMap);

      for (int entryIndex = 0; entryIndex < totalIndexEntries; ++entryIndex) {
        // READ THE KEY
        final int keyEntryCount = (int) uniqueKeysBuffer.getUnsignedNumber();
        final Object[] keyValues = new Object[keyEntryCount];
        for (int k = 0; k < keyEntryCount; ++k) {
          final byte keyType = uniqueKeysBuffer.getByte();
          keyValues[k] = serializer.deserializeValue(database, uniqueKeysBuffer, keyType);
        }

        final int totalKeyEntries = (int) uniqueKeysBuffer.getUnsignedNumber();

        final Set<TransactionIndexContext.IndexKey> values = new HashSet<>(totalKeyEntries);
        indexMap.put(new TransactionIndexContext.ComparableKey(keyValues), values);

        for (int i = 0; i < totalKeyEntries; ++i) {
          final boolean addOperation = uniqueKeysBuffer.getByte() == 1;

          final RID rid = new RID(database, (int) uniqueKeysBuffer.getUnsignedNumber(), uniqueKeysBuffer.getUnsignedNumber());

          values.add(new TransactionIndexContext.IndexKey(addOperation, keyValues, rid));
        }
      }
    }

    return indexesMap;
  }
}
