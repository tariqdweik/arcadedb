/*
 * Copyright (c) 2018 - Arcade Analytics LTD (https://arcadeanalytics.com)
 */
package com.arcadedb.server.ha.message;

import com.arcadedb.database.Binary;
import com.arcadedb.exception.ConfigurationException;
import com.arcadedb.server.ArcadeDBServer;
import com.arcadedb.server.ha.ReplicationMessage;
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
  }

  public void serializeCommand(final HACommand command, final Binary buffer, final long messageNumber) {
    buffer.clear();
    buffer.putByte(getCommandId(command));
    buffer.putLong(messageNumber);
    command.toStream(buffer);
    buffer.flip();
  }

  public Pair<ReplicationMessage, HACommand> deserializeCommand(final Binary buffer, final byte[] requestBytes) {
    buffer.clear();
    buffer.putByteArray(requestBytes);
    buffer.flip();

    final byte commandId = buffer.getByte();

    final HACommand request = createCommandInstance(commandId);

    if (request != null) {
      final long messageNumber = buffer.getLong();
      request.fromStream(buffer);

      buffer.rewind();
      return new Pair<>(new ReplicationMessage(messageNumber, buffer), request);
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
