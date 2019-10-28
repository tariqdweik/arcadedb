/*
 * Copyright (c) - Arcade Analytics LTD (https://arcadeanalytics.com)
 */

package com.arcadedb.server.http.handler;

import com.arcadedb.database.Database;
import com.arcadedb.serializer.JsonSerializer;
import com.arcadedb.server.ServerMetrics;
import com.arcadedb.server.http.HttpServer;
import com.arcadedb.sql.executor.ResultSet;
import io.undertow.server.HttpServerExchange;
import org.json.JSONObject;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

public class PostQueryHandler extends DatabaseAbstractHandler {
    public PostQueryHandler(final HttpServer httpServer) {
        super(httpServer);
    }

    @Override
    public void execute(final HttpServerExchange exchange, final Database database) throws IOException {

        final String payload = parseRequestPayload(exchange);
        if (payload == null || payload.isEmpty()) {
            exchange.setStatusCode(400);
            exchange.getResponseSender().send("{ \"error\" : \"Command text is null\"}");
            return;
        }

        final JSONObject json = new JSONObject(payload);

        final Map<String, Object> requestMap = json.toMap();

        final String language = (String) requestMap.get("language");

        final String command = (String) requestMap.get("command");

        if (command == null || command.isEmpty()) {
            exchange.setStatusCode(400);
            exchange.getResponseSender().send("{ \"error\" : \"Command text is null\"}");
            return;
        }

        final Map<String, Object> paramMap = (Map<String, Object>) requestMap.get("params");

        final ServerMetrics.MetricTimer timer = httpServer.getServer().getServerMetrics().timer("http.command");

        try {

            final ResultSet qResult = command(database, language, command, paramMap);

            final JsonSerializer serializer = httpServer.getJsonSerializer();

            final String result = qResult.stream()
                    .map(r -> serializer.serializeResult(r).toString())
                    .collect(Collectors.joining());

            exchange.setStatusCode(200);
            exchange.getResponseSender().send("{ \"result\" : [" + result + "] }");

        } finally {
            timer.stop();
        }

    }

    private ResultSet command(Database database, String language, String command, Map<String, Object> paramMap) {
        Object params = mapParams(paramMap);

        if (params instanceof Object[])
            return database.query(language, command, (Object[]) params);

        return database.query(language, command, (Map<String, Object>) params);
    }

    private Object mapParams(Map<String, Object> paramMap) {
        if (paramMap != null) {
            if (!paramMap.isEmpty() && paramMap.containsKey("0")) {
                // ORDINAL
                final Object[] array = new Object[paramMap.size()];
                for (int i = 0; i < array.length; ++i) {
                    array[i] = paramMap.get("" + i);
                }
                return array;
            }
        }
        return Optional.ofNullable(paramMap).orElse(Collections.emptyMap());
    }
}