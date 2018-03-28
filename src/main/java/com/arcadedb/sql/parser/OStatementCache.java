package com.arcadedb.sql.parser;

import com.arcadedb.database.PDatabase;
import com.arcadedb.exception.PCommandSQLParsingException;
import com.arcadedb.utility.PLogManager;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.io.UnsupportedEncodingException;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * This class is an LRU cache for already parsed SQL statement executors. It stores itself in the storage as a resource. It also
 * acts an an entry point for the SQL parser.
 *
 * @author Luigi Dell'Aquila (l.dellaquila-(at)-orientdb.com)
 */
public class OStatementCache {

  Map<String, OStatement> map;
  int                     mapSize;

  /**
   * @param size the size of the cache
   */
  public OStatementCache(int size) {
    this.mapSize = size;
    map = new LinkedHashMap<String, OStatement>(size) {
      protected boolean removeEldestEntry(final Map.Entry<String, OStatement> eldest) {
        return super.size() > mapSize;
      }
    };
  }

  /**
   * @param statement an SQL statement
   *
   * @return true if the corresponding executor is present in the cache
   */
  public boolean contains(String statement) {
    synchronized (map) {
      return map.containsKey(statement);
    }
  }

  /**
   * returns an already parsed SQL executor, taking it from the cache if it exists or creating a new one (parsing and then putting
   * it into the cache) if it doesn't
   *
   * @param statement the SQL statement
   * @param db        the current DB instance. If null, cache is ignored and a new executor is created through statement parsing
   *
   * @return a statement executor from the cache
   */
  public static OStatement get(String statement, PDatabase db) {
//    if (db == null) {
      return parse(statement, db);
//    }
//
//    OStatementCache resource = db.getSharedContext().getStatementCache();
//    return resource.get(statement);
  }

  /**
   * parses an SQL statement and returns the corresponding executor
   *
   * @param statement the SQL statement
   *
   * @return the corresponding executor
   *
   * @throws PCommandSQLParsingException if the input parameter is not a valid SQL statement
   */
  protected static OStatement parse(String statement, PDatabase db) throws PCommandSQLParsingException {
    try {

      InputStream is;

      if (db == null) {
        is = new ByteArrayInputStream(statement.getBytes());
      } else {
        try {

          is = new ByteArrayInputStream(statement.getBytes("UTF-8"));
//          is = new ByteArrayInputStream(statement.getBytes(db.getStorage().getConfiguration().getCharset()));
        } catch (UnsupportedEncodingException e2) {
          PLogManager.instance()
              .warn(null, "Unsupported charset for database " + db);
          is = new ByteArrayInputStream(statement.getBytes());
        }
      }

      OrientSql osql = null;
      if (db == null) {
        osql = new OrientSql(is);
      } else {
        try {
//          osql = new OrientSql(is, db.getStorage().getConfiguration().getCharset());
          osql = new OrientSql(is, "UTF-8");
        } catch (UnsupportedEncodingException e2) {
          PLogManager.instance()
              .warn(null, "Unsupported charset for database " + db );
          osql = new OrientSql(is);
        }
      }
      OStatement result = osql.parse();
      result.originalStatement = statement;

      return result;
    } catch (ParseException e) {
      throwParsingException(e, statement);
    } catch (TokenMgrError e2) {
      throwParsingException(e2, statement);
    }
    return null;
  }

  protected static void throwParsingException(ParseException e, String statement) {
    throw new PCommandSQLParsingException(statement, e);
  }

  protected static void throwParsingException(TokenMgrError e, String statement) {
    throw new PCommandSQLParsingException(statement, e);
  }

  public void clear() {
    synchronized (map) {
      map.clear();
    }
  }
}
