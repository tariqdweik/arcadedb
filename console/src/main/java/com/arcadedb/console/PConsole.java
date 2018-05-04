package com.arcadedb.console;

import com.arcadedb.database.PDatabase;
import com.arcadedb.database.PDatabaseFactory;
import com.arcadedb.engine.PPaginatedFile;
import com.arcadedb.sql.executor.OResultSet;
import org.beryx.textio.system.SystemTextTerminal;

public class PConsole {
  private final SystemTextTerminal textIO = new SystemTextTerminal();
  private       PDatabase          database;

  public PConsole() {

    textIO.print("ArcadeDB Console v1.0 (https://arcadeanalytics.com)\n");
    while (true) {
      textIO.print("\n> ");

      String line = textIO.read(true);
      line = line.trim();

      if (line.equalsIgnoreCase("quit") || line.equalsIgnoreCase("exit")) {
        break;
      } else if (line.startsWith("close")) {
        if (database != null) {
          database.close();
          database = null;
        }
      } else if (line.startsWith("open")) {
        database = new PDatabaseFactory(line.substring("connect".length()).trim(), PPaginatedFile.MODE.READ_WRITE).acquire();
      } else if (line.startsWith("select")) {
        if (database == null) {
          textIO.print("No active database. Open a database first\n");
          continue;
        }

        final OResultSet result = database.query(line.substring("select".length()).trim(), null);
        final PTableFormatter table = new PTableFormatter(new PTableFormatter.OTableOutput() {
          @Override
          public void onMessage(String text, Object... args) {
            textIO.print(text);
          }
        });

//        table.writeRecords(result);
      }
    }
  }

  public static void main(String[] args) {
    new PConsole();
  }
}
