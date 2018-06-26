/*
 * Copyright (c) 2018 - Arcade Analytics LTD (https://arcadeanalytics.com)
 */
package com.arcadedb.server.ha.message;

import com.arcadedb.exception.ConfigurationException;
import com.arcadedb.utility.LogManager;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class HAMessageFactory {
  private final List<Class<? extends HACommand>>      commands   = new ArrayList<>();
  private final Map<Class<? extends HACommand>, Byte> commandMap = new HashMap<>();

  public HAMessageFactory() {
    registerCommand(DatabaseListRequest.class);
    registerCommand(DatabaseListResponse.class);
    registerCommand(DatabaseStructureRequest.class);
    registerCommand(DatabaseStructureResponse.class);
    registerCommand(FileContentRequest.class);
    registerCommand(FileContentResponse.class);
    registerCommand(TxRequest.class);
    registerCommand(TxResponse.class);
    registerCommand(ReplicaReadyRequest.class);
  }

  public HACommand getCommand(final byte type) {
    if (type > commands.size())
      throw new IllegalArgumentException("Command with id " + type + " was not found");

    try {
      return commands.get(type).newInstance();
    } catch (Exception e) {
      LogManager.instance().error(this, "Error on creating replication command", e);
      throw new ConfigurationException("Error on creating replication command", e);
    }
  }

  public byte getCommandId(final HACommand command) {
    Byte commandId = commandMap.get(command.getClass());
    if (commandId == null)
      throw new IllegalArgumentException("Command of class " + command.getClass() + " was not found");

    return commandId;
  }

  private void registerCommand(final Class<? extends HACommand> commandClass) {
    commands.add(commandClass);
    commandMap.put(commandClass, (byte) (commands.size() - 1));
  }
}
