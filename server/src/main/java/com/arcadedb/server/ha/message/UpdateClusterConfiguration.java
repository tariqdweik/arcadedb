/*
 * Copyright (c) 2018 - Arcade Analytics LTD (https://arcadeanalytics.com)
 */
package com.arcadedb.server.ha.message;

import com.arcadedb.database.Binary;
import com.arcadedb.server.ha.HAServer;

import java.util.logging.Level;

public class UpdateClusterConfiguration extends HAAbstractCommand {
  private String servers;

  public UpdateClusterConfiguration() {
  }

  public UpdateClusterConfiguration(final String servers) {
    this.servers = servers;
  }

  @Override
  public HACommand execute(final HAServer server, final String remoteServerName, final long messageNumber) {
    server.getServer().log(this, Level.INFO, "Updating server list to: %s", servers);
    server.setServerAddresses(servers);
    return null;
  }

  @Override
  public void toStream(final Binary stream) {
    stream.putString(servers);
  }

  @Override
  public void fromStream(final Binary stream) {
    servers = stream.getString();
  }

  @Override
  public String toString() {
    return "updateClusterConfig(servers=" + servers + ")";
  }
}
