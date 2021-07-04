/*
 * Copyright (c) - Arcade Data LTD (https://arcadedata.com)
 */
package com.arcadedb.postgresw;

import com.arcadedb.Constants;
import com.arcadedb.database.Binary;
import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.exception.CommandSQLParsingException;
import com.arcadedb.exception.DatabaseOperationException;
import com.arcadedb.exception.QueryParsingException;
import com.arcadedb.log.LogManager;
import com.arcadedb.network.binary.ChannelBinaryServer;
import com.arcadedb.server.ArcadeDBServer;
import com.arcadedb.server.ServerSecurityException;
import com.arcadedb.sql.executor.Result;
import com.arcadedb.sql.executor.ResultSet;
import com.arcadedb.sql.executor.SQLEngine;
import com.arcadedb.utility.FileUtils;
import com.arcadedb.utility.Pair;

import java.io.EOFException;
import java.io.IOException;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.Level;

public class PostgresNetworkExecutor extends Thread {
  public enum ERROR_SEVERITY {FATAL, ERROR}

  public static        String                                         PG_SERVER_VERSION = "10.5";
  private static final int                                            BUFFER_LENGTH     = 32 * 1024;
  private final        ArcadeDBServer                                 server;
  private              Database                                       database;
  private              ChannelBinaryServer                            channel;
  private volatile     boolean                                        shutdown          = false;
  private              int                                            posInBuffer       = 0;
  private final        byte[]                                         buffer            = new byte[BUFFER_LENGTH];
  private              int                                            bytesRead         = 0;
  private              int                                            nextByte          = 0;
  private              boolean                                        reuseLastByte     = false;
  private              String                                         userName          = null;
  private              String                                         databaseName      = null;
  private              String                                         userPassword      = null;
  private              byte                                           transactionStatus = 'I';
  private              int                                            consecutiveErrors = 0;
  private              long                                           processIdSequence = 0;
  private              Map<Long, Pair<Long, PostgresNetworkExecutor>> activeSessions    = new ConcurrentHashMap<>();
  private              Map<String, PostgresPortal>                    portals           = new HashMap<>();
  private              boolean                                        DEBUG             = true;

  private interface ReadMessageCallback {
    void read(char type, long length) throws IOException;
  }

  enum TYPES {
    SMALLINT(21, Short.class, 2, -1), //
    INTEGER(23, Integer.class, 4, -1), //
    LONG(20, Long.class, 8, -1), //
    REAL(700, Float.class, 4, -1), //
    DOUBLE(701, Double.class, 8, -1), //
    CHAR(18, Character.class, 1, -1), //
    BOOLEAN(16, Boolean.class, 1, -1),  //
    DATE(1082, Date.class, 8, -1), //
    VARCHAR(1043, String.class, -1, -1), //
    ANY(2276, Object.class, 4, -1) //
    ;

    public final int      code;
    public final Class<?> cls;
    public final int      size;
    public final int      modifier;

    TYPES(final int code, final Class<?> cls, final int size, final int modifier) {
      this.code = code;
      this.cls = cls;
      this.size = size;
      this.modifier = modifier;
    }

    public void serialize(final ByteBuffer typeBuffer, final Object value) {
      if (value == null) {
        typeBuffer.putInt(-1);
        return;
      }

      switch (this) {
      case SMALLINT:
        typeBuffer.putInt(Binary.SHORT_SERIALIZED_SIZE);
        typeBuffer.putShort(((Number) value).shortValue());
        break;

      case INTEGER:
        typeBuffer.putInt(Binary.INT_SERIALIZED_SIZE);
        typeBuffer.putInt(((Number) value).intValue());
        break;

      case LONG:
        typeBuffer.putInt(Binary.LONG_SERIALIZED_SIZE);
        typeBuffer.putLong(((Number) value).longValue());
        break;

      case DOUBLE:
        typeBuffer.putInt(Binary.LONG_SERIALIZED_SIZE);
        typeBuffer.putDouble(((Number) value).doubleValue());
        break;

      case DATE:
        typeBuffer.putInt(Binary.LONG_SERIALIZED_SIZE);
        typeBuffer.putLong(((Date) value).getTime());
        break;

      case CHAR:
        typeBuffer.putInt(Binary.BYTE_SERIALIZED_SIZE);
        typeBuffer.put((byte) ((Character) value).charValue());
        break;

      case BOOLEAN:
        typeBuffer.putInt(Binary.BYTE_SERIALIZED_SIZE);
        typeBuffer.put((byte) (((Boolean) value) ? 1 : 0));
        break;

      case VARCHAR:
        final byte[] str = value.toString().getBytes();
        typeBuffer.putInt(str.length);
        typeBuffer.put(str);
        break;

      case ANY:
        typeBuffer.putInt(Binary.INT_SERIALIZED_SIZE);
        typeBuffer.putInt(((Number) value).intValue());
        break;

      default:
        throw new PostgresProtocolException("Type " + toString() + " not supported for serializing");
      }
    }
  }

  private interface WriteMessageCallback {
    void write() throws IOException;
  }

  public PostgresNetworkExecutor(final ArcadeDBServer server, final Socket socket, final Database database) throws IOException {
    setName(Constants.PRODUCT + "-postgres/" + socket.getInetAddress());
    this.server = server;
    this.channel = new ChannelBinaryServer(socket, server.getConfiguration());
    this.database = database;
  }

  public void close() {
    shutdown = true;
    if (channel != null)
      channel.close();
  }

  @Override
  public void run() {
    try {
      if (!readStartupMessage(true))
        return;

      writeMessage("request for password", () -> {
        channel.writeUnsignedInt(3);
      }, 'R', 8);

      waitForAMessage();

      readMessage("password", (type, length) -> {
        userPassword = readString();
      }, 'p');

      if (!openDatabase())
        return;

      writeMessage("authentication ok", () -> {
        channel.writeUnsignedInt(0);
      }, 'R', 8);

      sendServerParameter("server_version", PG_SERVER_VERSION);
      sendServerParameter("server_encoding", "UTF8");
      sendServerParameter("client_encoding", "UTF8");

      final long pid = processIdSequence++;
      final long secret = new Random().nextInt();

      // BackendKeyData
      writeMessage("backend key data", () -> {
        channel.writeUnsignedInt((int) pid);
        channel.writeUnsignedInt((int) secret);
      }, 'K', 12);

      activeSessions.put(pid, new Pair<>(secret, this));

      try {
        writeReadyForQueryMessage();

        while (!shutdown) {
          try {

            readMessage("any", (type, length) -> {
              consecutiveErrors = 0;

              switch (type) {
              case 'P':
                parseCommand();
                break;

              case 'B':
                bindCommand();
                break;

              case 'E':
                executeCommand();
                break;

              case 'Q':
                queryCommand();
                break;

              case 'S':
                syncCommand();
                break;

              case 'D':
                describeCommand();
                break;

              case 'C':
                closeCommand();
                break;

              case 'X':
                // TERMINATE
                shutdown = true;
                return;

              default:
                throw new PostgresProtocolException("Message '" + type + "' not managed");
              }

            }, 'D', 'P', 'B', 'E', 'Q', 'S', 'C', 'X');

          } catch (Exception e) {
            server.log(this, Level.SEVERE, "Postgres wrapper: Error on reading request: %s", e, e.getMessage());
            if (++consecutiveErrors > 3) {
              server.log(this, Level.SEVERE, "Closing connection with client");
              return;
            }
          }
        }
      } finally {
        activeSessions.remove(pid);
      }

    } finally {
      close();
    }
  }

  private void syncCommand() {
    if (DEBUG)
      LogManager.instance().log(this, Level.INFO, "PSQL: sync");

    writeReadyForQueryMessage();
  }

  private void describeCommand() throws IOException {
    final byte type = channel.readByte();
    final String portalName = readString();

    if (DEBUG)
      LogManager.instance().log(this, Level.INFO, "PSQL: describe '%s' type=%s", null, portalName, (char) type);

    final PostgresPortal portal = portals.get(portalName);

    // TRANSFORM PARAMETERS
    final Object[] parameters = new Object[0];

    final ResultSet resultSet = portal.statement.execute(database, parameters);
    portal.cachedResultset = browseAndCacheResultset(resultSet);
    portal.columns = getColumns(portal.cachedResultset);

    writeRowDescription(portal.columns);
  }

  private void closeCommand() throws IOException {
    final byte closeType = channel.readByte();
    final String prepStatementOrPortal = readString();

    portals.remove(prepStatementOrPortal);

    if (DEBUG)
      LogManager.instance().log(this, Level.INFO, "PSQL: close '%s' type=%s", null, prepStatementOrPortal, (char) closeType);

    writeMessage("close complete", null, '3', 4);
  }

  private void executeCommand() {
    try {
      final String portalName = readString();
      final int limit = (int) channel.readUnsignedInt();

      final PostgresPortal portal = portals.remove(portalName);

      if (DEBUG)
        LogManager.instance().log(this, Level.INFO, "PSQL: execute '%s' (limit=%d)-> %s", null, portalName, limit, portal);

      if (portal.ignoreExecution)
        writeMessage("empty query response", null, 'I', 4);
      else {
        // TRANSFORM PARAMETERS
        final Object[] parameters = new Object[0];

        if (portal.cachedResultset == null) {
          final ResultSet resultSet = portal.statement.execute(database, parameters);
          portal.cachedResultset = browseAndCacheResultset(resultSet);
          portal.columns = getColumns(portal.cachedResultset);
        }

        writeDataRows(portal.cachedResultset, portal.columns);
        writeCommandComplete(portal.query, portal.cachedResultset.size());
      }

    } catch (QueryParsingException | CommandSQLParsingException e) {
      writeError(ERROR_SEVERITY.ERROR, "Syntax error on executing query: " + e.getCause().getMessage(), "42601");
    } catch (Exception e) {
      writeError(ERROR_SEVERITY.ERROR, "Error on executing query: " + e.getMessage(), "XX000");
    } finally {
      writeReadyForQueryMessage();
    }
  }

  private void queryCommand() {
    try {
      String queryText = readString().trim();
      if (queryText.endsWith(";"))
        queryText = queryText.substring(0, queryText.length() - 1);

      if (DEBUG)
        LogManager.instance().log(this, Level.INFO, "PSQL: query -> %s", null, queryText);

      if (queryText.isEmpty()) {

        writeMessage("empty query response", null, 'I', 4);

      } else {

        final ResultSet resultSet = database.command("sql", queryText);

        final List<Result> cachedResultset = browseAndCacheResultset(resultSet);

        final Map<String, TYPES> columns = getColumns(cachedResultset);

        writeRowDescription(columns);

        writeDataRows(cachedResultset, columns);

        writeCommandComplete(queryText, cachedResultset.size());
      }

    } catch (QueryParsingException | CommandSQLParsingException e) {
      writeError(ERROR_SEVERITY.ERROR, "Syntax error on executing query: " + e.getCause().getMessage(), "42601");
    } catch (Exception e) {
      writeError(ERROR_SEVERITY.ERROR, "Error on executing query: " + e.getMessage(), "XX000");
    } finally {
      writeReadyForQueryMessage();
    }
  }

  private void writeReadyForQueryMessage() {
    writeMessage("ready for query", () -> {
      channel.writeByte(transactionStatus);
    }, 'Z', 5);
  }

  private List<Result> browseAndCacheResultset(ResultSet resultSet) {
    final List<Result> cachedResultset = new ArrayList<>();
    while (resultSet.hasNext()) {
      final Result row = resultSet.next();
      if (row == null)
        continue;

      cachedResultset.add(row);
    }
    return cachedResultset;
  }

  private Map<String, TYPES> getColumns(final List<Result> resultSet) {
    final Map<String, TYPES> columns = new LinkedHashMap<>();

    for (Result row : resultSet) {
      final Set<String> propertyNames = row.getPropertyNames();
      for (String p : propertyNames) {
        final Object value = row.getProperty(p);
        if (value != null) {
          TYPES valueType = columns.get(p);

          if (valueType == null) {
            // FIND THE VALUE TYPE AND WRITE IT IN THE DATA DESCRIPTION
            final Class valueClass = value.getClass();

            for (TYPES t : TYPES.values()) {
              if (t.cls.isAssignableFrom(valueClass)) {
                valueType = t;
                break;
              }
            }

            if (valueType == null)
              valueType = TYPES.ANY;

            columns.put(p, valueType);
          }
        }
      }
    }

    return columns;
  }

  private void writeRowDescription(final Map<String, TYPES> columns) {
    final ByteBuffer bufferDescription = ByteBuffer.allocate(64 * 1024);

    for (Map.Entry<String, TYPES> col : columns.entrySet()) {
      final String columnName = col.getKey();
      final TYPES columnType = col.getValue();

      bufferDescription.put(columnName.getBytes());//The field name.
      bufferDescription.put((byte) 0);

      bufferDescription.putInt(0); //If the field can be identified as a column of a specific table, the object ID of the table; otherwise zero.
      bufferDescription
          .putShort((short) 0); //If the field can be identified as a column of a specific table, the attribute number of the column; otherwise zero.
      bufferDescription.putInt(columnType.code);// The object ID of the field's data type.
      bufferDescription.putShort((short) columnType.size);// The data type size (see pg_type.typlen). Note that negative values denote variable-width types.
      bufferDescription.putInt(columnType.modifier);// The type modifier (see pg_attribute.atttypmod). The meaning of the modifier is type-specific.
      bufferDescription.putShort(
          (short) 1); // The format code being used for the field. Currently will be zero (text) or one (binary). In a RowDescription returned from the statement variant of Describe, the format code is not yet known and will always be zero.
    }

    bufferDescription.flip();
    writeMessage("row description", () -> {
      channel.writeUnsignedShort((short) columns.size());
      channel.writeBuffer(bufferDescription);
    }, 'T', 4 + 2 + bufferDescription.limit());
  }

  private void writeDataRows(final List<Result> resultSet, final Map<String, TYPES> columns) throws IOException {
    final ByteBuffer bufferData = ByteBuffer.allocate(64 * 1024);
    final ByteBuffer bufferValues = ByteBuffer.allocate(64 * 1024);

    for (Result row : resultSet) {
      bufferValues.clear();
      bufferValues.putShort((short) columns.size()); // Int16 The number of column values that follow (possibly zero).

      for (Map.Entry<String, TYPES> entry : columns.entrySet()) {
        final String propertyName = entry.getKey();
        final Object value = row.getProperty(propertyName);

        entry.getValue().serialize(bufferValues, value);
      }

      bufferValues.flip();
      bufferData.put((byte) 'D');
      bufferData.putInt(4 + bufferValues.limit());
      bufferData.put(bufferValues);
    }

    bufferData.flip();
    channel.writeBuffer(bufferData);
    channel.flush();

    if (DEBUG)
      LogManager.instance().log(this, Level.INFO, "PSQL:-> %d row data (%s)", null, resultSet.size(), FileUtils.getSizeAsString(bufferData.limit()));
  }

  private void bindCommand() {
    try {
      // BIND
      final String portalName = readString();
      final String sourcePreparedStatement = readString();

      final PostgresPortal portal = portals.get(portalName);

      if (DEBUG)
        LogManager.instance().log(this, Level.INFO, "PSQL: bind '%s' -> %s", null, portalName, sourcePreparedStatement);

      final int paramFormatCount = channel.readShort();
      if (paramFormatCount > 0) {
        portal.parameterFormats = new ArrayList<>(paramFormatCount);
        for (int i = 0; i < paramFormatCount; i++) {
          final int formatCode = channel.readUnsignedShort();
          portal.parameterFormats.add(formatCode);
        }
      }

      final int paramValuesCount = channel.readShort();
      if (paramValuesCount > 0) {
        portal.parameterValues = new ArrayList<>(paramValuesCount);
        for (int i = 0; i < paramValuesCount; i++) {
          final long paramSize = channel.readUnsignedInt();
          final byte[] paramValue = new byte[(int) paramSize];
          channel.readBytes(paramValue);

          portal.parameterValues.add(paramValue);
        }
      }

      final int resultFormatCount = channel.readShort();
      if (resultFormatCount > 0) {
        portal.resultFormats = new ArrayList<>(resultFormatCount);
        for (int i = 0; i < resultFormatCount; i++) {
          final int resultFormat = channel.readUnsignedShort();
          portal.resultFormats.add(resultFormat);
        }
      }

      writeMessage("bind complete", null, '2', 4);

    } catch (Exception e) {
      writeError(ERROR_SEVERITY.ERROR, "Error on parsing bind message: " + e.getMessage(), "XX000");
    }
  }

  private void parseCommand() {
    try {
      // PARSE
      final PostgresPortal portal = new PostgresPortal();

      final String portalName = readString();
      portal.query = readString();
      final int paramCount = channel.readShort();

      if (paramCount > 0) {
        portal.parameterTypes = new ArrayList<>(paramCount);
        for (int i = 0; i < paramCount; i++) {
          final long param = channel.readUnsignedInt();
          portal.parameterTypes.add(param);
        }
      }

      if (DEBUG)
        LogManager.instance().log(this, Level.INFO, "PSQL: parse '%s' %s(%d)", null, portalName, portal.query, paramCount);

      final String upperCaseText = portal.query.toUpperCase();
      if (upperCaseText.startsWith("SET "))
        portal.ignoreExecution = true;
      else
        portal.statement = SQLEngine.parse(portal.query, (DatabaseInternal) database);

      portals.put(portalName, portal);

      // ParseComplete
      writeMessage("parse complete", null, '1', 4);

    } catch (QueryParsingException | CommandSQLParsingException e) {
      writeError(ERROR_SEVERITY.ERROR, "Syntax error on parsing query: " + e.getCause().getMessage(), "42601");
    } catch (Exception e) {
      writeError(ERROR_SEVERITY.ERROR, "Error on parsing query: " + e.getMessage(), "XX000");
    }
  }

  private void sendServerParameter(final String name, final String value) {
    final byte[] nameBytes = name.getBytes(StandardCharsets.UTF_8);
    final byte[] valueBytes = value.getBytes(StandardCharsets.UTF_8);

    final int length = 4 + nameBytes.length + 1 + valueBytes.length + 1;

    writeMessage("parameter status", () -> {
      writeString(name);
      writeString(value);
    }, 'S', length);
  }

  private boolean openDatabase() {
    if (databaseName == null) {
      writeError(ERROR_SEVERITY.FATAL, "Database not selected", "HV00Q");
      return false;
    }

    try {
      server.getSecurity().authenticate(userName, userPassword);

      database = server.getDatabase(databaseName);
      database.setAutoTransaction(true);
    } catch (ServerSecurityException e) {
      writeError(ERROR_SEVERITY.FATAL, "Credentials not valid", "28P01");
      return false;
    } catch (DatabaseOperationException e) {
      writeError(ERROR_SEVERITY.FATAL, "Database not exists", "HV00Q");
      return false;
    }

    return true;
  }

  private boolean readStartupMessage(final boolean no2ssl) {
    try {
      final long len = channel.readUnsignedInt();
      final long protocolVersion = channel.readUnsignedInt();
      if (protocolVersion == 80877103) {
        // REQUEST FOR SSL, NOT SUPPORTED
        if (no2ssl) {
          channel.writeByte((byte) 'N');
          channel.flush();

          // REPEAT
          return readStartupMessage(false);
        }

        throw new PostgresProtocolException("SSL authentication is not supported");
      } else if (protocolVersion == 80877102) {
        // CANCEL REQUEST, IGNORE IT
        final long pid = channel.readUnsignedInt();
        final long secret = channel.readUnsignedInt();

        LogManager.instance().log(this, Level.INFO, "Received cancel request pid %d", null, pid);

        final Pair<Long, PostgresNetworkExecutor> session = activeSessions.get(pid);
        if (session != null) {
          if (session.getFirst() == secret)
            LogManager.instance().log(this, Level.INFO, "Canceling session " + pid);
          else
            LogManager.instance().log(this, Level.INFO, "Blocked unhautorized canceling session " + pid);
        } else
          LogManager.instance().log(this, Level.INFO, "Session " + pid + " not found");

        close();
        return false;
      }

      if (len > 8) {
        while (readNextByte() != 0) {
          reuseLastByte();

          final String paramName = readString();
          final String paramValue = readString();

          switch (paramName) {
          case "user":
            userName = paramValue;
            break;
          case "database":
            databaseName = paramValue;
            break;
          case "options":
            // DEPRECATED, IGNORE IT
            break;
          case "replication":
            // NOT SUPPORTED, IGNORE IT
            break;
          }
        }
      }
    } catch (IOException e) {
      throw new PostgresProtocolException("Error on parsing startup message", e);
    }
    return true;
  }

  private void writeError(final ERROR_SEVERITY severity, final String errorMessage, final String errorCode) {
    try {
      final String sev = severity.toString();

      int length = 4 + //
          1 + errorMessage.getBytes(StandardCharsets.UTF_8).length + 1 +//
          1 + sev.getBytes(StandardCharsets.UTF_8).length + 1 +//
          1 + errorCode.getBytes(StandardCharsets.UTF_8).length + 1 +//
          1;

      channel.writeByte((byte) 'E');
      channel.writeUnsignedInt(length);

      channel.writeByte((byte) 'M');
      writeString(errorMessage);

      channel.writeByte((byte) 'S');
      writeString(sev);

      channel.writeByte((byte) 'C');
      writeString(errorCode);

      channel.writeByte((byte) 0);
      channel.flush();
    } catch (IOException e) {
      throw new PostgresProtocolException("Error on sending error '" + errorMessage + "' to the client", e);
    }
  }

  private void writeMessage(final String messageName, final WriteMessageCallback callback, final char messageCode, final long length) {
    try {
      channel.writeByte((byte) messageCode);
      channel.writeUnsignedInt((int) length);
      if (callback != null)
        callback.write();
      channel.flush();

      if (DEBUG)
        LogManager.instance().log(this, Level.INFO, "PSQL:-> %s (%s - %s)", null, messageName, messageCode, FileUtils.getSizeAsString(length));

    } catch (IOException e) {
      throw new PostgresProtocolException("Error on sending " + messageName + " message", e);
    }
  }

  private void readMessage(final String messageName, final ReadMessageCallback callback, final char... expectedMessageCodes) {
    try {
      final char type = (char) readNextByte();
      final long length = channel.readUnsignedInt();

      if (expectedMessageCodes != null && expectedMessageCodes.length > 0) {
        // VALIDATE MESSAGES
        boolean valid = false;
        for (int i = 0; i < expectedMessageCodes.length; i++) {
          if (type == expectedMessageCodes[i]) {
            valid = true;
            break;
          }
        }

        if (!valid) {
          // READ TILL THE END OF THE MESSAGE
          readBytes((int) (length - 4));
          throw new PostgresProtocolException("Unexpected message type '" + type + "' for message " + messageName);
        }
      }

      if (length > 4)
        callback.read(type, length - 4);

    } catch (EOFException e) {
      // CLIENT CLOSES THE CONNECTION
      return;
    } catch (IOException e) {
      throw new PostgresProtocolException("Error on reading " + messageName + " message: " + e.getMessage(), e);
    }
  }

  private int readNextByte() throws IOException {
    if (reuseLastByte) {
      // USE THE BYTE ALREADY READ
      reuseLastByte = false;
      return nextByte;
    }

    return nextByte = channel.readUnsignedByte();
  }

  private void waitForAMessage() {
    while (!channel.inputHasData()) {
      try {
        Thread.sleep(100);
      } catch (InterruptedException interruptedException) {
        throw new PostgresProtocolException("Error on reading from the channel");
      }
    }
  }

  private void reuseLastByte() {
    reuseLastByte = true;
  }

  private String readString() throws IOException {
    int len = 0;
    for (; len < buffer.length; len++) {
      final int b = readNextByte();
      if (b == 0)
        return new String(buffer, 0, len);

      buffer[len] = (byte) b;
    }

    len = readUntilTerminator(len);

    throw new PostgresProtocolException("String content (" + len + ") too long (>" + BUFFER_LENGTH + ")");
  }

  private void writeString(final String text) throws IOException {
    channel.writeBytes(text.getBytes(StandardCharsets.UTF_8));
    channel.writeByte((byte) 0);
  }

  private int readUntilTerminator(int len) throws IOException {
    // OUT OF BUFFER SIZE, CONTINUE READING AND DISCARD THE CONTENT
    for (; readNextByte() != 0; len++) {
    }
    return len;
  }

  private void readBytes(int len) throws IOException {
    for (int i = 0; i < len; i++)
      readNextByte();
  }

  private void writeCommandComplete(final String queryText, final int resultSetCount) {
    final String upperCaseText = queryText.toUpperCase();
    String tag = "";
    if (upperCaseText.startsWith("CREATE VERTEX") || upperCaseText.startsWith("INSERT INTO"))
      tag = "INSERT 0 ";
    else if (upperCaseText.startsWith("SELECT") || upperCaseText.startsWith("MATCH"))
      tag = "SELECT ";
    else if (upperCaseText.startsWith("UPDATE"))
      tag = "UPDATE ";
    else if (upperCaseText.startsWith("DELETE"))
      tag = "DELETE ";

    tag += resultSetCount;

    String finalTag = tag;
    writeMessage("command complete", () -> {
      writeString(finalTag);
    }, 'C', 4 + tag.length() + 1);
  }
}
