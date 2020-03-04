/*
 * Copyright (c) - Arcade Data LTD (https://arcadedata.com)
 */
package com.arcadedb.sql.function.misc;

import com.arcadedb.database.Identifiable;
import com.arcadedb.exception.CommandSQLParsingException;
import com.arcadedb.sql.executor.CommandContext;
import com.arcadedb.sql.function.SQLFunctionAbstract;

import java.util.Base64;

/**
 * Encode a string in various format (only base64 for now)
 *
 * @author Johann Sorel (Geomatys)
 */
public class SQLFunctionDecode extends SQLFunctionAbstract {

  public static final String NAME = "decode";

  /**
   * Get the date at construction to have the same date for all the iteration.
   */
  public SQLFunctionDecode() {
    super(NAME, 2, 2);
  }

  @Override
  public Object execute( final Object iThis, Identifiable iCurrentRecord, Object iCurrentResult,
      final Object[] iParams, final CommandContext iContext) {

    final String candidate = iParams[0].toString();
    final String format = iParams[1].toString();

    if (SQLFunctionEncode.FORMAT_BASE64.equalsIgnoreCase(format)) {
      return Base64.getDecoder().decode(candidate);
    } else {
      throw new CommandSQLParsingException("Unknowned format :" + format);
    }
  }

  @Override
  public String getSyntax() {
    return "decode(<binaryfield>, <format>)";
  }
}
