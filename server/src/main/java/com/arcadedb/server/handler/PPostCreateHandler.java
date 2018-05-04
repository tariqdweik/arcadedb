package com.arcadedb.server.handler;

import com.arcadedb.database.PDatabase;
import com.arcadedb.server.PHttpServer;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

public class PPostCreateHandler extends PBasicHandler {
  public PPostCreateHandler(final PHttpServer pHttpServer) {
    super(pHttpServer);
  }

  @Override
  public String getURL() {
    return "/create";
  }

  @Override
  public void execute(final HttpServletRequest request, final HttpServletResponse response, final PDatabase database) {
//    try {
//      final Deque<String> recordInJson = exchange.getQueryParameters().get("record");
//      if (recordInJson.isEmpty()) {
//        exchange.setStatusCode(400);
//        exchange.getResponseSender().send("{ \"error\" : \"Record is null\"}");
//        return;
//      }
//
////      final PDocument record =
////
////      exchange.setStatusCode(200);
////      exchange.getResponseSender()
////          .send("{ \"result\" : " + httpServer.getJsonSerializer().serializeRecord(record).toString() + "}");
//
//    } catch (PRecordNotFoundException e) {
//      exchange.setStatusCode(404);
//      exchange.getResponseSender().send("{ \"error\" : \"Record id is null\"}");
//    }
  }
}
