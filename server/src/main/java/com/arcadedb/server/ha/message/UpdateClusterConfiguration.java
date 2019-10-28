/*
 * Copyright (c) - Arcade Analytics LTD (https://arcadeanalytics.com)
 */
package com.arcadedb.server.ha.message;

import com.arcadedb.database.Binary;
import com.arcadedb.server.ha.HAServer;

import java.util.logging.Level;

public class UpdateClusterConfiguration extends HAAbstractCommand {
  private String servers;
  private String replicaServersHTTPAddresses;

  public UpdateClusterConfiguration() {
  }

  public UpdateClusterConfiguration(final String servers, final String replicaServersHTTPAddresses) {
    this.servers = servers;
    this.replicaServersHTTPAddresses = replicaServersHTTPAddresses;
  }

  @Override
  public HACommand execute(final HAServer server, final String remoteServerName, final long messageNumber) {
    server.getServer().log(this, Level.FINE, "Updating server list=%s replicaHTTPs=%s", servers, replicaServersHTTPAddresses);
    server.setServerAddresses(servers);
    server.setReplicasHTTPAddresses(replicaServersHTTPAddresses);
    return null;
  }

  @Override
  public void toStream(final Binary stream) {
    stream.putString(servers);
    stream.putString(replicaServersHTTPAddresses);
  }

  @Override
  public void fromStream(final Binary stream) {
    servers = stream.getString();
    replicaServersHTTPAddresses = stream.getString();
  }

  @Override
  public String toString() {
    return "updateClusterConfig(servers=" + servers + ")";
  }
}
