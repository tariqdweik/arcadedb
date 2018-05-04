package com.arcadedb.server.handler;

import com.arcadedb.database.PDatabase;
import com.arcadedb.server.PHttpServer;
import com.arcadedb.server.PServerCommandException;
import com.arcadedb.sql.executor.OResultSet;
import com.arcadedb.utility.PFileUtils;
import org.json.JSONObject;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.net.URLDecoder;

public class PPostCommandHandler extends PBasicHandler {
  public PPostCommandHandler(final PHttpServer httpServer) {
    super(httpServer);
  }

  @Override
  public void execute(final HttpServletRequest request, final HttpServletResponse response, final PDatabase database) {
    try {
      String command;

      final int prefixLength = (getURL() + "/" + database.getName()).length();
      if (request.getRequestURI().length() > prefixLength) {
        command = request.getRequestURI().substring(prefixLength + 1);
        command = URLDecoder.decode(command, "UTF-8");

      } else {

        final String payload = PFileUtils.readStreamAsString(request.getInputStream(), request.getCharacterEncoding());
        if (payload.isEmpty()) {
          response.sendError(400, "{ \"error\" : \"Command text id is null\"}");
          return;
        }

        final JSONObject jsonPayload = new JSONObject(payload);

        command = (String) jsonPayload.get("command");
        if (command == null || command.isEmpty()) {
          response.sendError(400, "{ \"error\" : \"Command text id is null\"}");
          return;
        }
      }

      final OResultSet qResult = database.query(command, null);

      final StringBuilder result = new StringBuilder();
      while (qResult.hasNext()) {
        if (result.length() > 0)
          result.append(",");
        result.append(httpServer.getJsonSerializer().serializeResult(qResult.next()).toString());
      }

      response.setStatus(200);
      response.getWriter().write("{ \"result\" : [" + result.toString() + "] }");

    } catch (Exception e) {
      throw new PServerCommandException("Error on executing command (database=" + database.getName() + ")");
    }
  }

  @Override
  public String getURL() {
    return "/command";
  }
}
