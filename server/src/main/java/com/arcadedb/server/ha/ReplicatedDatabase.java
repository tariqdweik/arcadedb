/*
 * Copyright (c) 2018 - Arcade Analytics LTD (https://arcadeanalytics.com)
 */

package com.arcadedb.server.ha;

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.database.*;
import com.arcadedb.database.async.DatabaseAsyncExecutor;
import com.arcadedb.engine.*;
import com.arcadedb.graph.Edge;
import com.arcadedb.graph.GraphEngine;
import com.arcadedb.graph.ModifiableVertex;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.Schema;
import com.arcadedb.serializer.BinarySerializer;
import com.arcadedb.server.ArcadeDBServer;
import com.arcadedb.server.ha.message.CheckpointRequest;
import com.arcadedb.server.ha.message.TxRequest;
import com.arcadedb.sql.executor.ResultSet;
import com.arcadedb.sql.parser.ExecutionPlanCache;
import com.arcadedb.sql.parser.StatementCache;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Level;

public class ReplicatedDatabase implements DatabaseInternal {
  private final ArcadeDBServer      server;
  private final EmbeddedDatabase    proxied;
  private       Binary              buffer             = new Binary(4096);
  private       AtomicLong          messageNumber      = new AtomicLong();
  private       long                lastCheckpoint     = 0;
  private       Long[]              lastMessage        = new Long[] { -1l, -1l, -1l };
  private       Map<String, Long[]> replicaCheckpoints = new HashMap<>();
  private       HAServer.QUORUM     quorum;
  private final long                timeout;

  public ReplicatedDatabase(final ArcadeDBServer server, final EmbeddedDatabase proxied) {
    this.server = server;
    this.proxied = proxied;
    this.quorum = HAServer.QUORUM.valueOf(proxied.getConfiguration().getValueAsString(GlobalConfiguration.HA_QUORUM).toUpperCase());
    this.timeout = proxied.getConfiguration().getValueAsLong(GlobalConfiguration.HA_QUORUM_TIMEOUT);
  }

  public EmbeddedDatabase getEmbeddedDatabase() {
    return proxied;
  }

  @Override
  public void close() {
    proxied.close();
  }

  @Override
  public void drop() {
    throw new UnsupportedOperationException("Server proxied database instance cannot be drop");
  }

  @Override
  public void registerCallback(final CALLBACK_EVENT event, final Callable<Void> callback) {
    proxied.registerCallback(event, callback);
  }

  @Override
  public void unregisterCallback(final CALLBACK_EVENT event, final Callable<Void> callback) {
    proxied.unregisterCallback(event, callback);
  }

  @Override
  public void executeCallbacks(final CALLBACK_EVENT event) throws IOException {
    proxied.executeCallbacks(event);
  }

  @Override
  public GraphEngine getGraphEngine() {
    return proxied.getGraphEngine();
  }

  @Override
  public TransactionManager getTransactionManager() {
    return proxied.getTransactionManager();
  }

  @Override
  public void createRecord(final ModifiableDocument record) {
    proxied.createRecord(record);
  }

  @Override
  public void createRecord(final Record record, final String bucketName) {
    proxied.createRecord(record, bucketName);
  }

  @Override
  public void createRecordNoLock(final Record record, final String bucketName) {
    proxied.createRecordNoLock(record, bucketName);
  }

  @Override
  public void updateRecord(final Record record) {
    proxied.updateRecord(record);
  }

  @Override
  public void updateRecordNoLock(final Record record) {
    proxied.updateRecordNoLock(record);
  }

  @Override
  public void indexDocument(final ModifiableDocument record, final DocumentType type, final Bucket bucket) {
    proxied.indexDocument(record, type, bucket);
  }

  @Override
  public void kill() {
    proxied.kill();
  }

  @Override
  public WALFileFactory getWALFileFactory() {
    return proxied.getWALFileFactory();
  }

  @Override
  public StatementCache getStatementCache() {
    return proxied.getStatementCache();
  }

  @Override
  public ExecutionPlanCache getExecutionPlanCache() {
    return proxied.getExecutionPlanCache();
  }

  @Override
  public String getName() {
    return proxied.getName();
  }

  @Override
  public DatabaseAsyncExecutor asynch() {
    return proxied.asynch();
  }

  @Override
  public String getDatabasePath() {
    return proxied.getDatabasePath();
  }

  @Override
  public TransactionContext getTransaction() {
    return proxied.getTransaction();
  }

  @Override
  public boolean isTransactionActive() {
    return proxied.isTransactionActive();
  }

  @Override
  public boolean checkTransactionIsActive() {
    return proxied.checkTransactionIsActive();
  }

  @Override
  public void transaction(final Transaction txBlock) {
    proxied.transaction(txBlock);
  }

  @Override
  public void setAutoTransaction(final boolean autoTransaction) {
    proxied.setAutoTransaction(autoTransaction);
  }

  @Override
  public void begin() {
    proxied.begin();
  }

  @Override
  public void commit() {
    proxied.executeInReadLock(new Callable<Object>() {
      @Override
      public Object call() {
        proxied.checkTransactionIsActive();
        final Binary changes = proxied.getTransaction().commit();

        if (changes != null) {
          server.log(this, Level.FINE, "Replicating transaction (size=%d)", changes.size());

          try {

            final int activeServers = 1 + server.getHA().getOnlineReplicas();

            final int reqQuorum;
            switch (quorum) {
            case NONE:
              reqQuorum = 0;
              break;
            case ONE:
              reqQuorum = 1;
              break;
            case TWO:
              reqQuorum = 2;
              break;
            case THREE:
              reqQuorum = 3;
              break;
            case MAJORITY:
              reqQuorum = (activeServers / 2) + 1;
              break;
            case ALL:
              reqQuorum = activeServers;
              break;
            default:
              throw new IllegalArgumentException("Quorum " + quorum + " not managed");
            }

            server.getHA().sendCommandToReplicasWithQuorum(buffer,
                new TxRequest(messageNumber.getAndIncrement(), getName(), changes, reqQuorum > 1), reqQuorum, timeout);

          } catch (IOException e) {
            server.log(this, Level.SEVERE, "Error on replicating transaction (error:%s)", e);
          }

          if (System.currentTimeMillis() - lastCheckpoint > 1000)
            executeCheckpoint();
        }

        return null;
      }
    });
  }

  private void executeCheckpoint() {
    try {
      server.getHA().sendCommandToReplicas(buffer, new CheckpointRequest(getName()));
      lastCheckpoint = System.currentTimeMillis();
    } catch (IOException e) {
      server.log(this, Level.SEVERE, "Error on executing checkpoint (error:%s)", e);
    }
  }

  @Override
  public void rollback() {
    proxied.rollback();
  }

  @Override
  public void scanType(final String className, final boolean polymorphic, final DocumentCallback callback) {
    proxied.scanType(className, polymorphic, callback);
  }

  @Override
  public void scanBucket(final String bucketName, final RecordCallback callback) {
    proxied.scanBucket(bucketName, callback);
  }

  @Override
  public Record lookupByRID(final RID rid, final boolean loadContent) {
    return proxied.lookupByRID(rid, loadContent);
  }

  @Override
  public Iterator<Record> iterateType(final String typeName, final boolean polymorphic) {
    return proxied.iterateType(typeName, polymorphic);
  }

  @Override
  public Iterator<Record> iterateBucket(final String bucketName) {
    return proxied.iterateBucket(bucketName);
  }

  @Override
  public Cursor<RID> lookupByKey(final String type, final String[] properties, final Object[] keys) {
    return proxied.lookupByKey(type, properties, keys);
  }

  @Override
  public void deleteRecord(final Record record) {
    proxied.deleteRecord(record);
  }

  @Override
  public long countType(final String typeName, final boolean polymorphic) {
    return proxied.countType(typeName, polymorphic);
  }

  @Override
  public long countBucket(final String bucketName) {
    return proxied.countBucket(bucketName);
  }

  @Override
  public ModifiableDocument newDocument(final String typeName) {
    return proxied.newDocument(typeName);
  }

  @Override
  public ModifiableVertex newVertex(String typeName) {
    return proxied.newVertex(typeName);
  }

  @Override
  public Edge newEdgeByKeys(final String sourceVertexType, final String[] sourceVertexKey, final Object[] sourceVertexValue,
      final String destinationVertexType, final String[] destinationVertexKey, final Object[] destinationVertexValue,
      final boolean createVertexIfNotExist, final String edgeType, final boolean bidirectional, final Object... properties) {

    return proxied.newEdgeByKeys(sourceVertexType, sourceVertexKey, sourceVertexValue, destinationVertexType, destinationVertexKey,
        destinationVertexValue, createVertexIfNotExist, edgeType, bidirectional, properties);
  }

  @Override
  public Schema getSchema() {
    return proxied.getSchema();
  }

  @Override
  public FileManager getFileManager() {
    return proxied.getFileManager();
  }

  @Override
  public void transaction(final Transaction txBlock, final int retries) {
    proxied.transaction(txBlock, retries);
  }

  @Override
  public RecordFactory getRecordFactory() {
    return proxied.getRecordFactory();
  }

  @Override
  public BinarySerializer getSerializer() {
    return proxied.getSerializer();
  }

  @Override
  public PageManager getPageManager() {
    return proxied.getPageManager();
  }

  @Override
  public ResultSet sql(final String query, final Map<String, Object> args) {
    return proxied.sql(query, args);
  }

  @Override
  public ResultSet query(final String query, final Map<String, Object> args) {
    return proxied.query(query, args);
  }

  @Override
  public Object executeInReadLock(final Callable<Object> callable) {
    return proxied.executeInReadLock(callable);
  }

  @Override
  public Object executeInWriteLock(final Callable<Object> callable) {
    return proxied.executeInWriteLock(callable);
  }

  @Override
  public boolean isReadYourWrites() {
    return proxied.isReadYourWrites();
  }

  @Override
  public void setReadYourWrites(final boolean value) {
    proxied.setReadYourWrites(value);
  }

  public Long[] getLastMessage() {
    return lastMessage;
  }

  public void updateLastMessage(final Long[] ids) {
    lastMessage = ids;
  }

  public Long[] getReplicaCheckpoint(final String replicaName) {
    return replicaCheckpoints.get(replicaName);
  }

  public void updateReplicaCheckpoint(final String replicaName, final Long[] values) {
    replicaCheckpoints.put(replicaName, values);
  }
}
