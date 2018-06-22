/*
 * Copyright (c) 2018 - Arcade Analytics LTD (https://arcadeanalytics.com)
 */
package com.arcadedb.sql.function.text;

import com.arcadedb.database.Identifiable;
import com.arcadedb.exception.CommandExecutionException;
import com.arcadedb.sql.executor.CommandContext;
import com.arcadedb.sql.method.misc.OAbstractSQLMethod;

import java.io.UnsupportedEncodingException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

/**
 * Hash a string supporting multiple algorithm, all those supported by JVM
 *
 * @author Luca Garulli (l.garulli--(at)--orientdb.com)
 */
public class SQLMethodHash extends OAbstractSQLMethod {

  public static final String NAME = "hash";

  public static final String HASH_ALGORITHM = "SHA-256";

  public SQLMethodHash() {
    super(NAME, 0, 1);
  }

  @Override
  public String getSyntax() {
    return "hash([<algorithm>])";
  }

  @Override
  public Object execute(final Object iThis, final Identifiable iCurrentRecord, final CommandContext iContext, final Object ioResult,
      final Object[] iParams) {
    if (iThis == null)
      return null;

    final String algorithm = iParams.length > 0 ? iParams[0].toString() : HASH_ALGORITHM;
    try {
      return createHash(iThis.toString(), algorithm);

    } catch (NoSuchAlgorithmException e) {
      throw new CommandExecutionException("hash(): algorithm '" + algorithm + "' is not supported", e);
    } catch (UnsupportedEncodingException e) {
      throw new CommandExecutionException("hash(): encoding 'UTF-8' is not supported", e);
    }
  }

  public static String createHash(final String iInput, String iAlgorithm)
      throws NoSuchAlgorithmException, UnsupportedEncodingException {
    if (iAlgorithm == null)
      iAlgorithm = HASH_ALGORITHM;

    final MessageDigest msgDigest = MessageDigest.getInstance(iAlgorithm);

    return byteArrayToHexStr(msgDigest.digest(iInput.getBytes("UTF-8")));
  }

  public static String byteArrayToHexStr(final byte[] data) {
    if (data == null)
      return null;

    final char[] chars = new char[data.length * 2];
    for (int i = 0; i < data.length; i++) {
      final byte current = data[i];
      final int hi = (current & 0xF0) >> 4;
      final int lo = current & 0x0F;
      chars[2 * i] = (char) (hi < 10 ? ('0' + hi) : ('A' + hi - 10));
      chars[2 * i + 1] = (char) (lo < 10 ? ('0' + lo) : ('A' + lo - 10));
    }
    return new String(chars);
  }

}
