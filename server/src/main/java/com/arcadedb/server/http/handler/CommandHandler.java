/*
 * Copyright (c) 2018 - Arcade Analytics LTD (https://arcadeanalytics.com)
 */

package com.arcadedb.server.http.handler;

import com.arcadedb.database.Database;
import com.arcadedb.log.LogManager;
import com.arcadedb.serializer.JsonSerializer;
import com.arcadedb.server.ServerMetrics;
import com.arcadedb.server.http.HttpServer;
import com.arcadedb.sql.executor.ResultSet;
import io.undertow.server.HttpServerExchange;
import org.json.JSONObject;

import java.io.IOException;
import java.util.Map;
import java.util.logging.Level;
import java.util.stream.Collectors;

public class CommandHandler extends DatabaseAbstractHandler {
    public CommandHandler(final HttpServer httpServer) {
        super(httpServer);
    }

    @Override
    public void execute(final HttpServerExchange exchange, final Database database) throws IOException {
        Object[] params = null;

        final String payload = parseRequestPayload(exchange);
        if (payload == null || payload.isEmpty()) {
            exchange.setStatusCode(400);
            exchange.getResponseSender().send("{ \"error\" : \"Command text is null\"}");
            return;
        }

        final JSONObject json = new JSONObject(payload);

        LogManager.instance().log(this, Level.INFO, "json:: " + json.toString());

        final Map<String, Object> requestMap = json.toMap();

        final String language = (String) requestMap.get("language");

        final String command = (String) requestMap.get("command");

        if (command == null || command.isEmpty()) {
            exchange.setStatusCode(400);
            exchange.getResponseSender().send("{ \"error\" : \"Command text is null\"}");
            return;
        }

        final Map<String, Object> paramMap = (Map<String, Object>) requestMap.get("params");
        if (paramMap != null) {
            if (!paramMap.isEmpty() && paramMap.containsKey("0")) {
                // ORDINAL
                final Object[] array = new Object[paramMap.size()];
                for (int i = 0; i < array.length; ++i) {
                    array[i] = paramMap.get("" + i);
                }
                params = array;
            } else
                params = new Object[]{paramMap};
        }


        final ServerMetrics.MetricTimer timer = httpServer.getServer().getServerMetrics().timer("http.command");

        String result;
        try {

            final ResultSet qResult = database.command(language, command, params);

            final JsonSerializer serializer = httpServer.getJsonSerializer();


            result = qResult.stream().map(r -> serializer.serializeResult(r).toString()).collect(Collectors.joining());
//            while (qResult.hasNext()) {
//                if (result.length() > 0)
//                    result.append(",");
//                final Result record = qResult.next();
//
////                LogManager.instance().log(this, Level.INFO, "record:: " + record);
//
//                result.append(serializer.serializeResult(record).toString());
//            }

        } finally {
            timer.stop();
        }

        exchange.setStatusCode(200);
        exchange.getResponseSender().send("{ \"result\" : [" + result + "] }");
    }
}