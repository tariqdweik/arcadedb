package com.arcadedb.sql.executor;

import com.arcadedb.database.PEmbeddedDatabase;
import com.arcadedb.sql.function.ODefaultSQLFunctionFactory;
import com.arcadedb.sql.parser.OStatementCache;
import com.arcadedb.sql.parser.Statement;

public class OSQLEngine {
  private static final OSQLEngine                 INSTANCE = new OSQLEngine();
  private final        ODefaultSQLFunctionFactory functions;

  protected OSQLEngine() {
    functions = new ODefaultSQLFunctionFactory();
  }

  public static OSQLEngine getInstance() {
    return INSTANCE;
  }

  public OSQLFunction getFunction(String name) {
    return functions.createFunction(name);
  }

  public static OSQLMethod getMethod(String name) {
    return null;
  }

  public static Statement parse(String query, PEmbeddedDatabase pDatabase) {
    return OStatementCache.get(query, pDatabase);
  }
}
