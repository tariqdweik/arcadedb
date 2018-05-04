package com.arcadedb.console;

import com.arcadedb.PConstants;
import com.arcadedb.database.PDatabase;
import com.arcadedb.database.PDatabaseFactory;
import com.arcadedb.database.PDocument;
import com.arcadedb.engine.PPaginatedFile;
import com.arcadedb.graph.PEdge;
import com.arcadedb.graph.PVertex;
import com.arcadedb.schema.PDocumentType;
import com.arcadedb.sql.executor.OResultSet;
import org.jline.reader.*;
import org.jline.terminal.Terminal;
import org.jline.terminal.TerminalBuilder;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

public class PConsole {
  private static final String          PROMPT = "\n%s> ";
  private final        boolean         system = System.console() != null;
  private final        Terminal        terminal;
  private final        LineReader      lineReader;
  private final        PTerminalParser parser = new PTerminalParser();
  private              PDatabase       database;
  private              PConsoleOutput  output;

  private String getPrompt() {
    return String.format(PROMPT, database != null ? "{" + database.getName() + "}" : "");
  }

  public PConsole(final boolean interactive) throws IOException {
    terminal = TerminalBuilder.builder().system(system).streams(System.in, System.out).jansi(true).build();
    lineReader = LineReaderBuilder.builder().terminal(terminal).parser(parser).build();

    output("ArcadeDB Console v.%s - %s (%s)\n", PConstants.VERSION, PConstants.COPYRIGHT, PConstants.URL);

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
    new PConsole(true);
  }

  public void close() {
    //terminal.close();
    if (database != null)
      database.close();
  }

  public void setOutput(final PConsoleOutput output) {
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
      else {
        executeCommand(line);
      }
    }

    return true;
  }

  private void executeBegin() {
    checkDatabaseIsOpen();
    database.begin();
  }

  private void executeCommit() {
    checkDatabaseIsOpen();
    database.commit();
  }

  private void executeRollback() {
    checkDatabaseIsOpen();
    database.rollback();
  }

  private void executeClose() {
    if (database != null) {
      if (database.isTransactionActive())
        database.commit();

      database.close();
      database = null;
    }
  }

  private void executeConnect(String line) {
    final String url = line.substring("connect".length()).trim();
    if (database != null)
      terminal.writer().print("Database already connected, to connect to a different database close the current one first\n");
    else if (!url.isEmpty())
      database = new PDatabaseFactory(url, PPaginatedFile.MODE.READ_WRITE).setAutoTransaction(true).acquire();
  }

  private void executeCommand(String line) {
    checkDatabaseIsOpen();

    final OResultSet result = database.command(line, null);
    final PTableFormatter table = new PTableFormatter(new PTableFormatter.OTableOutput() {
      @Override
      public void onMessage(String text, Object... args) {
        output(text, args);
      }
    });
    table.setPrefixedColumns("@RID", "@TYPE");

    final List<PRecordTableFormatter.PTableRecordRow> list = new ArrayList<>();
    while (result.hasNext()) {
      final Optional<PDocument> next = result.next().getElement();

      if (next.isPresent())
        list.add(new PRecordTableFormatter.PTableRecordRow(next.get()));
    }

    table.writeRows(list, 20);
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

      final PTableFormatter table = new PTableFormatter(new PTableFormatter.OTableOutput() {
        @Override
        public void onMessage(String text, Object... args) {
          output(text, args);
        }
      });

      final List<PTableFormatter.PTableMapRow> rows = new ArrayList<>();
      for (PDocumentType type : database.getSchema().getTypes()) {
        final PTableFormatter.PTableMapRow row = new PTableFormatter.PTableMapRow();
        row.setField("NAME", type.getName());

        final byte kind = type.getType();
        if (kind == PDocument.RECORD_TYPE)
          row.setField("TYPE", "Document");
        else if (kind == PVertex.RECORD_TYPE)
          row.setField("TYPE", "Vertex");
        else if (kind == PEdge.RECORD_TYPE)
          row.setField("TYPE", "Edge");

        row.setField("PARENT TYPES", type.getParentTypes());
        row.setField("BUCKETS", type.getBuckets(false));
        row.setField("PROPERTIES", type.getPropertyNames());
        row.setField("SYNC STRATEGY", type.getSyncSelectionStrategy());
        row.setField("ASYNC STRATEGY", type.getAsyncSelectionStrategy());

        rows.add(row);
      }

      table.writeRows(rows, -1);
    }
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
    if (database == null)
      throw new RuntimeException("No active database. Open a database first\n");
  }
}
