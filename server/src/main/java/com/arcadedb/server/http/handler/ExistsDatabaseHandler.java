/*
 * Copyright (c) - Arcade Analytics LTD (https://arcadeanalytics.com)
 */

package com.arcadedb.server.http.handler;

import com.arcadedb.database.Database;
import com.arcadedb.server.ArcadeDBServer;
import com.arcadedb.server.http.HttpServer;
import io.undertow.server.HttpServerExchange;

import java.util.Deque;

public class ExistsDatabaseHandler extends DatabaseAbstractHandler {
    public ExistsDatabaseHandler(final HttpServer httpServer) {
        super(httpServer);
    }

    @Override
    protected boolean openDatabase() {
        return false;
    }

    @Override
    public void execute(final HttpServerExchange exchange, final Database database) {
        final Deque<String> databaseName = exchange.getQueryParameters().get("database");
        if (databaseName.isEmpty()) {
            exchange.setStatusCode(400);
            exchange.getResponseSender().send("{ \"error\" : \"Database parameter is null\"}");
            return;
        }

        final ArcadeDBServer server = httpServer.getServer();
        server.getServerMetrics().meter("http.exists-database").mark();

        final boolean existsDatabase = server.existsDatabase(databaseName.getFirst());

        exchange.setStatusCode(200);
        exchange.getResponseSender().send("{ \"result\" : \"" + existsDatabase + "\"}");
    }
}