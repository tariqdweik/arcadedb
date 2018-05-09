/*
 * Copyright (c) 2018 - Arcade Analytics LTD (https://arcadeanalytics.com)
 */
package com.arcadedb.sql.function.text;

import com.arcadedb.database.Identifiable;
import com.arcadedb.exception.CommandExecutionException;
import com.arcadedb.security.SecurityManager;
import com.arcadedb.sql.executor.CommandContext;
import com.arcadedb.sql.method.misc.OAbstractSQLMethod;

import java.io.UnsupportedEncodingException;
import java.security.NoSuchAlgorithmException;

/**
 * Hash a string supporting multiple algorithm, all those supported by JVM
 *
 * @author Luca Garulli (l.garulli--(at)--orientdb.com)
 */
public class SQLMethodHash extends OAbstractSQLMethod {

  public static final String NAME = "hash";

  public SQLMethodHash() {
    super(NAME, 0, 1);
  }

  @Override
  public String getSyntax() {
    return "hash([<algorithm>])";
  }

  @Override
  public Object execute( final Object iThis, final Identifiable iCurrentRecord,
      final CommandContext iContext, final Object ioResult, final Object[] iParams) {
    if (iThis == null)
      return null;

    final String algorithm = iParams.length > 0 ? iParams[0].toString() : SecurityManager.HASH_ALGORITHM;
    try {
      return SecurityManager.createHash(iThis.toString(), algorithm);

    } catch (NoSuchAlgorithmException e) {
      throw new CommandExecutionException("hash(): algorithm '" + algorithm + "' is not supported", e);
    } catch (UnsupportedEncodingException e) {
      throw new CommandExecutionException("hash(): encoding 'UTF-8' is not supported", e);
    }
  }
}
