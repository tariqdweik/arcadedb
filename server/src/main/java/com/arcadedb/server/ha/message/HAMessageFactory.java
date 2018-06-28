/*
 * Copyright (c) 2018 - Arcade Analytics LTD (https://arcadeanalytics.com)
 */
package com.arcadedb.server.ha.message;

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.database.Binary;
import com.arcadedb.engine.CompressionFactory;
import com.arcadedb.exception.ConfigurationException;
import com.arcadedb.server.ArcadeDBServer;
import com.arcadedb.utility.LogManager;
import com.arcadedb.utility.Pair;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;

public class HAMessageFactory {
  private final ArcadeDBServer                        server;
  private final List<Class<? extends HACommand>>      commands   = new ArrayList<>();
  private final Map<Class<? extends HACommand>, Byte> commandMap = new HashMap<>();
  private final long                                  compressionThreshold;

  public HAMessageFactory(final ArcadeDBServer server) {
    this.server = server;

    registerCommand(ReplicaConnectRequest.class);
    registerCommand(ReplicaConnectFullResyncResponse.class);
    registerCommand(ReplicaConnectHotResyncResponse.class);
    registerCommand(DatabaseStructureRequest.class);
    registerCommand(DatabaseStructureResponse.class);
    registerCommand(FileContentRequest.class);
    registerCommand(FileContentResponse.class);
    registerCommand(TxRequest.class);
    registerCommand(TxResponse.class);
    registerCommand(ReplicaReadyRequest.class);

    compressionThreshold = server.getConfiguration().getValueAsLong(GlobalConfiguration.HA_COMPRESSION_THRESHOLD);
  }

  public void serializeCommand(final HACommand command, final Binary buffer, final Binary tempBuffer, final long messageNumber) {
    tempBuffer.reset();
    command.toStream(tempBuffer);
    tempBuffer.flip();

    buffer.reset();
    buffer.putByte(getCommandId(command));
    buffer.putLong(messageNumber);

    if (compressionThreshold > 0 && tempBuffer.size() > compressionThreshold) {
      // COMPRESS IT
      buffer.putByte((byte) 1);
      buffer.putInt(tempBuffer.size());
      final Binary compressedBuffer = CompressionFactory.getDefault().compress(tempBuffer);
      buffer.putByteArray(compressedBuffer.getContent(), compressedBuffer.size());
    } else {
      buffer.putByte((byte) 0);
      buffer.putByteArray(tempBuffer.getContent(), tempBuffer.size());
    }

    buffer.flip();
  }

  public Pair<Long, HACommand> deserializeCommand(Binary buffer, byte[] requestBytes) {
    buffer.reset();
    buffer.putByteArray(requestBytes);
    buffer.flip();

    final byte commandId = buffer.getByte();

    final HACommand request = createCommandInstance(commandId);

    if (request != null) {
      final long messageNumber = buffer.getLong();
      final boolean compressed = buffer.getByte() == 1;
      if (compressed) {
        final int uncompressedLength = buffer.getInt();
        buffer = CompressionFactory.getDefault().decompress(buffer, uncompressedLength);
      }
      request.fromStream(buffer);
      return new Pair<Long, HACommand>(messageNumber, request);
    }

    server.log(this, Level.SEVERE, "Error on reading request, command %d not valid", commandId);
    return null;
  }

  private void registerCommand(final Class<? extends HACommand> commandClass) {
    commands.add(commandClass);
    commandMap.put(commandClass, (byte) (commands.size() - 1));
  }

  private HACommand createCommandInstance(final byte type) {
    if (type > commands.size())
      throw new IllegalArgumentException("Command with id " + type + " was not found");

    try {
      return commands.get(type).newInstance();
    } catch (Exception e) {
      LogManager.instance().error(this, "Error on creating replication command", e);
      throw new ConfigurationException("Error on creating replication command", e);
    }
  }

  private byte getCommandId(final HACommand command) {
    final Byte commandId = commandMap.get(command.getClass());
    if (commandId == null)
      throw new IllegalArgumentException("Command of class " + command.getClass() + " was not found");

    return commandId;
  }
}
