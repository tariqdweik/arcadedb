/*
 * Copyright (c) 2018 - Arcade Analytics LTD (https://arcadeanalytics.com)
 */
package com.arcadedb.server.ha.message;

import com.arcadedb.database.*;
import com.arcadedb.engine.CompressionFactory;
import com.arcadedb.engine.WALFile;
import com.arcadedb.exception.NeedRetryException;
import com.arcadedb.exception.TransactionException;
import com.arcadedb.serializer.BinarySerializer;
import com.arcadedb.serializer.BinaryTypes;
import com.arcadedb.server.ha.HAServer;
import com.arcadedb.server.ha.ReplicationException;
import com.arcadedb.utility.LogManager;

import java.util.HashMap;
import java.util.Map;

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
      final Map<String, Map<TransactionIndexContext.ComparableKey, TransactionIndexContext.IndexKey>> keysTx) {
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

    final WALFile.WALTransaction walTx = readTxFromBuffer();
    final Map<String, Map<TransactionIndexContext.ComparableKey, TransactionIndexContext.IndexKey>> keysTx = readIndexKeysFromBuffer(db);

    // FORWARDED FROM A REPLICA
    db.begin();
    final TransactionContext tx = db.getTransaction();

    try {
      tx.commitFromReplica(walTx, keysTx);
    } catch (NeedRetryException | TransactionException e) {
      return new ErrorResponse(e);
    } catch (Exception e) {
      LogManager.instance().error(this, "Error with the execution of the forwarded message %d", e, messageNumber);
      return new ErrorResponse(e);
    }

    return new TxForwardResponse();
  }

  @Override
  public String toString() {
    return "tx-forward(" + databaseName + ")";
  }

  protected void writeIndexKeysToBuffer(final DatabaseInternal database,
      final Map<String, Map<TransactionIndexContext.ComparableKey, TransactionIndexContext.IndexKey>> indexChanges) {
    final BinarySerializer serializer = database.getSerializer();

    uniqueKeysBuffer = new Binary();

    uniqueKeysBuffer.putNumber(indexChanges.size());

    for (Map.Entry<String, Map<TransactionIndexContext.ComparableKey, TransactionIndexContext.IndexKey>> entry : indexChanges.entrySet()) {
      uniqueKeysBuffer.putString(entry.getKey());
      final Map<TransactionIndexContext.ComparableKey, TransactionIndexContext.IndexKey> keys = entry.getValue();

      uniqueKeysBuffer.putNumber(keys.size());

      for (TransactionIndexContext.IndexKey key : keys.values()) {
        uniqueKeysBuffer.putByte((byte) (key.addOperation ? 1 : 0));

        uniqueKeysBuffer.putNumber(key.keyValues.length);
        for (int k = 0; k < key.keyValues.length; ++k) {
          final byte keyType = BinaryTypes.getTypeFromValue(key.keyValues[k]);
          uniqueKeysBuffer.putByte(keyType);
          serializer.serializeValue(uniqueKeysBuffer, keyType, key.keyValues[k]);
        }

        uniqueKeysBuffer.putNumber(key.rid.getBucketId());
        uniqueKeysBuffer.putNumber(key.rid.getPosition());
      }
    }

    uniqueKeysUncompressedLength = uniqueKeysBuffer.size();
    uniqueKeysBuffer.rewind();
    uniqueKeysBuffer = CompressionFactory.getDefault().compress(uniqueKeysBuffer);
  }

  protected Map<String, Map<TransactionIndexContext.ComparableKey, TransactionIndexContext.IndexKey>> readIndexKeysFromBuffer(final DatabaseInternal database) {
    final BinarySerializer serializer = database.getSerializer();

    uniqueKeysBuffer.position(0);

    final int indexesCount = (int) uniqueKeysBuffer.getNumber();

    final Map<String, Map<TransactionIndexContext.ComparableKey, TransactionIndexContext.IndexKey>> entries = new HashMap<>(indexesCount);

    for (int indexIdx = 0; indexIdx < indexesCount; ++indexIdx) {
      final String indexName = uniqueKeysBuffer.getString();

      final int keyCount = (int) uniqueKeysBuffer.getNumber();

      final Map<TransactionIndexContext.ComparableKey, TransactionIndexContext.IndexKey> keys = new HashMap<>(keyCount);

      entries.put(indexName, keys);

      for (int keyIdx = 0; keyIdx < keyCount; ++keyIdx) {
        final boolean addOperation = uniqueKeysBuffer.getByte() == 1;
        final int keyEntryCount = (int) uniqueKeysBuffer.getNumber();

        final Object[] keyValues = new Object[keyEntryCount];

        for (int k = 0; k < keyEntryCount; ++k) {
          final byte keyType = uniqueKeysBuffer.getByte();
          keyValues[k] = serializer.deserializeValue(database, uniqueKeysBuffer, keyType);
        }

        final RID rid = new RID(database, (int) uniqueKeysBuffer.getNumber(), uniqueKeysBuffer.getNumber());

        final TransactionIndexContext.IndexKey key = new TransactionIndexContext.IndexKey(addOperation, keyValues, rid);
        keys.put(new TransactionIndexContext.ComparableKey(keyValues), key);
      }
    }

    return entries;
  }
}
