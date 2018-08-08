/*
 * Copyright (c) 2018 - Arcade Analytics LTD (https://arcadeanalytics.com)
 */

package com.arcadedb.console;

import com.arcadedb.Constants;
import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.database.Document;
import com.arcadedb.graph.Edge;
import com.arcadedb.graph.Vertex;
import com.arcadedb.remote.RemoteDatabase;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.sql.executor.ResultSet;
import com.arcadedb.utility.RecordTableFormatter;
import com.arcadedb.utility.TableFormatter;
import org.jline.reader.*;
import org.jline.terminal.Terminal;
import org.jline.terminal.TerminalBuilder;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class Console {
  private static final String          PROMPT = "\n%s> ";
  private final        boolean         system = System.console() != null;
  private final        Terminal        terminal;
  private final        LineReader      lineReader;
  private final        TerminalParser  parser = new TerminalParser();
  private              RemoteDatabase  remoteDatabase;
  private              ConsoleOutput   output;
  private              DatabaseFactory databaseFactory;
  private              Database        database;

  private String getPrompt() {
    return String.format(PROMPT, database != null ? "{" + database.getName() + "}" : "");
  }

  public Console(final boolean interactive) throws IOException {
    terminal = TerminalBuilder.builder().system(system).streams(System.in, System.out).jansi(true).build();
    lineReader = LineReaderBuilder.builder().terminal(terminal).parser(parser).build();

    output("%s Console v.%s - %s (%s)\n", Constants.PRODUCT, Constants.VERSION, Constants.COPYRIGHT, Constants.URL);

    if (!interactive)
      return;

    try {
      while (true) {

        try {
          String line = lineReader.readLine(getPrompt());
          if (line == null)
            continue;

          if (!parse(line))
            return;

        } catch (UserInterruptException e) {
          return;
        } catch (EndOfFileException e) {
          return;
        } catch (Exception e) {
          terminal.writer().print("\nError: " + e + "\n");
        }
      }
    } finally {
      close();
    }
  }

  public static void main(String[] args) throws IOException {
    new Console(true);
  }

  public void close() {
    if (terminal != null)
      terminal.writer().flush();

    if (remoteDatabase != null) {
      remoteDatabase.close();
      remoteDatabase = null;
    }

    if (database != null) {
      database.close();
      database = null;
    }

    if (databaseFactory != null) {
      databaseFactory.close();
      databaseFactory = null;
    }
  }

  public void setOutput(final ConsoleOutput output) {
    this.output = output;
  }

  private boolean execute(final String line) throws IOException {
    if (line != null && !line.isEmpty()) {
      if (line.equalsIgnoreCase("quit") || line.equalsIgnoreCase("exit")) {
        executeClose();
        return false;
      } else if (line.startsWith("info"))
        executeInfo(line.substring("info".length()).trim());
      else if (line.startsWith("load"))
        executeLoad(line.substring("load".length()).trim());
      else if (line.equalsIgnoreCase("help") || line.equals("?"))
        executeHelp();
      else if (line.startsWith("begin"))
        executeBegin();
      else if (line.startsWith("commit"))
        executeCommit();
      else if (line.startsWith("rollback"))
        executeRollback();
      else if (line.startsWith("close"))
        executeClose();
      else if (line.startsWith("connect"))
        executeConnect(line);
      else if (line.startsWith("create database"))
        executeCreateDatabase(line);
      else if (line.startsWith("drop database"))
        executeDropDatabase(line);
      else {
        executeSQL(line);
      }
    }

    return true;
  }

  private void executeBegin() {
    checkDatabaseIsOpen();
    if (database != null)
      database.begin();
    else
      remoteDatabase.command("SQL", "begin");
  }

  private void executeCommit() {
    checkDatabaseIsOpen();
    if (database != null)
      database.commit();
    else
      remoteDatabase.command("SQL", "commit");
  }

  private void executeRollback() {
    checkDatabaseIsOpen();
    if (database != null)
      database.rollback();
    else
      remoteDatabase.command("SQL", "rollback");
  }

  private void executeClose() {
    if (database != null) {
      if (database.isTransactionActive())
        database.commit();

      database.close();
      database = null;
    }

    if (remoteDatabase != null) {
      remoteDatabase.close();
      remoteDatabase = null;
    }
  }

  private void executeConnect(final String line) {
    final String url = line.substring("connect".length()).trim();
    if (database != null || remoteDatabase != null)
      terminal.writer().print("Database already connected, to connect to a different database close the current one first\n");
    else if (!url.isEmpty()) {
      if (url.startsWith("remote:")) {
        connectToRemoteServer(url);

        terminal.writer().printf("\nConnected\n");
        terminal.writer().flush();

      } else {
        databaseFactory = new DatabaseFactory(url);
        database = databaseFactory.setAutoTransaction(true).open();
      }
    } else
      throw new ConsoleException("URL missing");
  }

  private void executeCreateDatabase(final String line) {
    final String url = line.substring("create database".length()).trim();
    if (database != null || remoteDatabase != null)
      terminal.writer().print("Database already connected, to connect to a different database close the current one first\n");
    else if (!url.isEmpty()) {
      if (url.startsWith("remote:")) {
        connectToRemoteServer(url);
        remoteDatabase.create();

        terminal.writer().printf("\nDatabase created\n");
        terminal.writer().flush();

      } else {
        databaseFactory = new DatabaseFactory(url);
        database = databaseFactory.setAutoTransaction(true).create();
      }
    } else
      throw new ConsoleException("URL missing");
  }

  private void executeDropDatabase(final String line) {
    final String url = line.substring("drop database".length()).trim();
    if (database != null || remoteDatabase != null)
      terminal.writer().print("Database already connected, to connect to a different database close the current one first\n");
    else if (!url.isEmpty()) {
      if (url.startsWith("remote:")) {
        connectToRemoteServer(url);
        remoteDatabase.drop();

        terminal.writer().printf("\nDatabase dropped\n");
        terminal.writer().flush();

      } else {
        databaseFactory = new DatabaseFactory(url);
        database = databaseFactory.setAutoTransaction(true).open();
        database.drop();
      }
    } else
      throw new ConsoleException("URL missing");

    remoteDatabase = null;
    database = null;
  }

  private void executeSQL(final String line) {
    checkDatabaseIsOpen();

    final long beginTime = System.currentTimeMillis();

    final ResultSet result;

    if (remoteDatabase != null)
      result = remoteDatabase.command("SQL", line);
    else
      result = database.command("SQL", line);

    final TableFormatter table = new TableFormatter(new TableFormatter.OTableOutput() {
      @Override
      public void onMessage(String text, Object... args) {
        output(text, args);
      }
    });
    table.setPrefixedColumns("@RID", "@TYPE");

    final List<RecordTableFormatter.PTableRecordRow> list = new ArrayList<>();
    while (result.hasNext()) {
      list.add(new RecordTableFormatter.PTableRecordRow(result.next()));
    }

    final long elapsed = System.currentTimeMillis() - beginTime;

    table.writeRows(list, 20);

    terminal.writer().printf("\nCommand executed in %dms\n", elapsed);
  }

  private void executeLoad(final String fileName) throws IOException {
    if (fileName.isEmpty())
      throw new RuntimeException("File name is empty");

    final File file = new File(fileName);
    if (!file.exists())
      throw new RuntimeException("File name '" + fileName + "' not found");

    final FileReader fr = new FileReader(file);
    BufferedReader bufferedReader = new BufferedReader(fr);

    while (bufferedReader.ready())
      parse(bufferedReader.readLine());
  }

  public boolean parse(final String line) throws IOException {
    final ParsedLine parsed = parser.parse(line, 0);

    for (String w : parsed.words()) {
      terminal.writer().printf(getPrompt() + w);
      if (!execute(w))
        return false;
    }
    return true;
  }

  private void output(final String text, final Object... args) {
    if (output != null)
      output.onOutput(String.format(text, args));
    else
      terminal.writer().printf(text, args);
  }

  private void executeInfo(final String subject) {
    checkDatabaseIsOpen();

    if (subject.equalsIgnoreCase("types")) {
      output("\nAVAILABLE TYPES");

      final TableFormatter table = new TableFormatter(new TableFormatter.OTableOutput() {
        @Override
        public void onMessage(String text, Object... args) {
          output(text, args);
        }
      });

      if (remoteDatabase != null) {
        executeSQL("select from metadata:schema");
        return;
      }

      final List<TableFormatter.PTableMapRow> rows = new ArrayList<>();
      for (DocumentType type : database.getSchema().getTypes()) {
        final TableFormatter.PTableMapRow row = new TableFormatter.PTableMapRow();
        row.setField("NAME", type.getName());

        final byte kind = type.getType();
        if (kind == Document.RECORD_TYPE)
          row.setField("TYPE", "Document");
        else if (kind == Vertex.RECORD_TYPE)
          row.setField("TYPE", "Vertex");
        else if (kind == Edge.RECORD_TYPE)
          row.setField("TYPE", "Edge");

        row.setField("PARENT TYPES", type.getParentTypes());
        row.setField("BUCKETS", type.getBuckets(false));
        row.setField("PROPERTIES", type.getPropertyNames());
        row.setField("SYNC STRATEGY", type.getSyncSelectionStrategy());
        row.setField("ASYNC STRATEGY", type.getAsyncSelectionStrategy());

        rows.add(row);
      }

      table.writeRows(rows, -1);
    } else
      throw new ConsoleException("Information about '" + subject + "' is not available");
  }

  private void executeHelp() {
    output("HELP\n");
    output("connect <path> -> connect to a database stored on <path>");
    output("close          -> close the database");
    output("begin          -> begins a new transaction");
    output("commit         -> commits current transaction");
    output("rollback       -> rollbacks current transaction");
    output("quit or exit   -> exit from the console");
    output("info           -> help");
    output("");
  }

  private void checkDatabaseIsOpen() {
    if (database == null && remoteDatabase == null)
      throw new RuntimeException("No active database. Open a database first\n");
  }

  private void connectToRemoteServer(final String url) {
    final String conn = url.substring("remote:".length());

    final String[] serverUserPassword = conn.split(" ");
    if (serverUserPassword.length != 3)
      throw new ConsoleException("URL username and password are missing");

    final String[] serverParts = serverUserPassword[0].split("/");
    if (serverParts.length != 2)
      throw new ConsoleException("Remote URL '" + url + "' not valid");

    String remoteServer;
    int remotePort;

    final int portPos = serverParts[0].indexOf(":");
    if (portPos < 0) {
      remoteServer = serverParts[0];
      remotePort = RemoteDatabase.DEFAULT_PORT;
    } else {
      remoteServer = serverParts[0].substring(0, portPos);
      remotePort = Integer.parseInt(serverParts[0].substring(portPos + 1));
    }

    remoteDatabase = new RemoteDatabase(remoteServer, remotePort, serverParts[1], serverUserPassword[1], serverUserPassword[2]);
  }
}
