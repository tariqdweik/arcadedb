/*
 * Copyright (c) - Arcade Data LTD (https://arcadedata.com)
 */
package com.arcadedb.sql.function.misc;

import com.arcadedb.database.*;
import com.arcadedb.exception.CommandSQLParsingException;
import com.arcadedb.sql.executor.CommandContext;
import com.arcadedb.sql.function.SQLFunctionAbstract;

import java.util.Base64;

/**
 * Encode a string in various format (only base64 for now).
 *
 * @author Johann Sorel (Geomatys)
 */
public class SQLFunctionEncode extends SQLFunctionAbstract {

  public static final String NAME          = "encode";
  public static final String FORMAT_BASE64 = "base64";

  /**
   * Get the date at construction to have the same date for all the iteration.
   */
  public SQLFunctionEncode() {
    super(NAME, 2, 2);
  }

  public Object execute( Object iThis, Identifiable iCurrentRecord, Object iCurrentResult,
      final Object[] iParams, CommandContext iContext) {

    final Object candidate = iParams[0];
    final String format = iParams[1].toString();

    byte[] data = null;
    if (candidate instanceof byte[]) {
      data = (byte[]) candidate;
    } else if (candidate instanceof RID) {
      final Record rec = ((RID) candidate).getRecord();
      if (rec instanceof Binary) {
        data = ((Binary) rec).toByteArray();
      }
    }

    if (data == null) {
      return null;
    }

    if (FORMAT_BASE64.equalsIgnoreCase(format)) {
      return Base64.getEncoder().encodeToString(data);
    } else {
      throw new CommandSQLParsingException("Unknown format :" + format);
    }
  }

  @Override
  public String getSyntax() {
    return "encode(<binaryfield>, <format>)";
  }
}
