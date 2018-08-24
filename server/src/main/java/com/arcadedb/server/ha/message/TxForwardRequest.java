/*
 * Copyright (c) 2018 - Arcade Analytics LTD (https://arcadeanalytics.com)
 */
package com.arcadedb.server.ha.message;

import com.arcadedb.database.*;
import com.arcadedb.engine.CompressionFactory;
import com.arcadedb.engine.WALFile;
import com.arcadedb.exception.NeedRetryException;
import com.arcadedb.exception.TransactionException;
import com.arcadedb.index.Index;
import com.arcadedb.serializer.BinarySerializer;
import com.arcadedb.serializer.BinaryTypes;
import com.arcadedb.server.ha.HAServer;
import com.arcadedb.server.ha.ReplicationException;
import com.arcadedb.utility.LogManager;

import java.util.ArrayList;
import java.util.List;

/**
 * Forward a transaction to the Leader server to be executed. Apart the TX content (like with TxRequest), unique keys list is
 * needed to assure the index unique constraint.
 */
public class TxForwardRequest extends TxRequestAbstract {
  private int    uniqueKeysUncompressedLength;
  private Binary uniqueKeysBuffer;

  public TxForwardRequest() {
  }

  public TxForwardRequest(final DatabaseInternal database, final Binary bufferChanges, final List<DocumentIndexer.IndexKey> keysTx) {
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
    final List<DocumentIndexer.IndexKey> keysTx = readIndexKeysFromBuffer(db);

    // FORWARDED FROM A REPLICA
    db.begin();
    final TransactionContext tx = db.getTransaction();

    try {
      tx.commitFromReplica(walTx, keysTx);
    } catch (NeedRetryException | TransactionException e) {
      LogManager.instance().error(this, "Error with the execution of the forwarded message %d", e, messageNumber);
      return new ErrorResponse(e);
    }

    return new TxForwardResponse();
  }

  @Override
  public String toString() {
    return "tx-forward(" + databaseName + ")";
  }

  protected void writeIndexKeysToBuffer(final DatabaseInternal database, final List<DocumentIndexer.IndexKey> keys) {
    final BinarySerializer serializer = database.getSerializer();

    uniqueKeysBuffer = new Binary();

    uniqueKeysBuffer.putNumber(keys.size());

    for (int i = 0; i < keys.size(); ++i) {
      final DocumentIndexer.IndexKey key = keys.get(i);

      uniqueKeysBuffer.putNumber(key.index.getFileId());
      uniqueKeysBuffer.putString(key.typeName);

      uniqueKeysBuffer.putNumber(key.keyNames.length);
      for (int k = 0; k < key.keyNames.length; ++k) {
        final byte keyType = BinaryTypes.getTypeFromValue(key.keyValues[k]);

        uniqueKeysBuffer.putString(key.keyNames[k]);
        uniqueKeysBuffer.putByte(keyType);
        serializer.serializeValue(uniqueKeysBuffer, keyType, key.keyValues[k]);
      }

      uniqueKeysBuffer.putNumber(key.rid.getBucketId());
      uniqueKeysBuffer.putNumber(key.rid.getPosition());
    }

    uniqueKeysUncompressedLength = uniqueKeysBuffer.size();
    uniqueKeysBuffer.rewind();
    uniqueKeysBuffer = CompressionFactory.getDefault().compress(uniqueKeysBuffer);
  }

  protected List<DocumentIndexer.IndexKey> readIndexKeysFromBuffer(final DatabaseInternal database) {
    final BinarySerializer serializer = database.getSerializer();

    uniqueKeysBuffer.position(0);

    final int keyCount = (int) uniqueKeysBuffer.getNumber();

    final List<DocumentIndexer.IndexKey> keys = new ArrayList<>(keyCount);

    for (int i = 0; i < keyCount; ++i) {
      final int indexFileId = (int) uniqueKeysBuffer.getNumber();
      final String typeName = uniqueKeysBuffer.getString();

      final int keyEntryCount = (int) uniqueKeysBuffer.getNumber();

      final String[] keyNames = new String[keyEntryCount];
      final Object[] keyValues = new Object[keyEntryCount];

      for (int k = 0; k < keyEntryCount; ++k) {
        keyNames[k] = uniqueKeysBuffer.getString();
        final byte keyType = uniqueKeysBuffer.getByte();
        keyValues[k] = serializer.deserializeValue(database, uniqueKeysBuffer, keyType);
      }

      final RID rid = new RID(database, (int) uniqueKeysBuffer.getNumber(), uniqueKeysBuffer.getNumber());

      final Index index = (Index) database.getSchema().getFileById(indexFileId);

      final DocumentIndexer.IndexKey key = new DocumentIndexer.IndexKey(index, typeName, keyNames, keyValues, rid);
      keys.add(key);
    }

    return keys;
  }
}
