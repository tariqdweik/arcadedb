/*
 *
 *  *  Copyright 2010-2016 OrientDB LTD (http://orientdb.com)
 *  *
 *  *  Licensed under the Apache License, Version 2.0 (the "License");
 *  *  you may not use this file except in compliance with the License.
 *  *  You may obtain a copy of the License at
 *  *
 *  *       http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  *  Unless required by applicable law or agreed to in writing, software
 *  *  distributed under the License is distributed on an "AS IS" BASIS,
 *  *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  *  See the License for the specific language governing permissions and
 *  *  limitations under the License.
 *  *
 *  * For more information: http://orientdb.com
 *
 */
package com.arcadedb.security;

import com.arcadedb.exception.ConfigurationException;
import com.arcadedb.utility.LogManager;

import java.io.UnsupportedEncodingException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

public class OSecurityManager {
  public static final String HASH_ALGORITHM        = "SHA-256";
  public static final String HASH_ALGORITHM_PREFIX = "{" + HASH_ALGORITHM + "}";

  public static final String PBKDF2_ALGORITHM        = "PBKDF2WithHmacSHA1";
  public static final String PBKDF2_ALGORITHM_PREFIX = "{" + PBKDF2_ALGORITHM + "}";

  public static final String PBKDF2_SHA256_ALGORITHM        = "PBKDF2WithHmacSHA256";
  public static final String PBKDF2_SHA256_ALGORITHM_PREFIX = "{" + PBKDF2_SHA256_ALGORITHM + "}";

  public static final int SALT_SIZE = 24;
  public static final int HASH_SIZE = 24;

  private static final OSecurityManager instance = new OSecurityManager();

  private MessageDigest md;

  public OSecurityManager() {
    try {
      md = MessageDigest.getInstance(HASH_ALGORITHM);
    } catch (NoSuchAlgorithmException e) {
      LogManager.instance().error(this, "Cannot use OSecurityManager", e);
    }
  }

  public static String createHash(final String iInput, String iAlgorithm)
      throws NoSuchAlgorithmException, UnsupportedEncodingException {
    if (iAlgorithm == null)
      iAlgorithm = HASH_ALGORITHM;

    final MessageDigest msgDigest = MessageDigest.getInstance(iAlgorithm);

    return byteArrayToHexStr(msgDigest.digest(iInput.getBytes("UTF-8")));
  }

  public static OSecurityManager instance() {
    return instance;
  }

  public String createSHA256(final String iInput) {
    return byteArrayToHexStr(digestSHA256(iInput));
  }

  public synchronized byte[] digestSHA256(final String iInput) {
    if (iInput == null)
      return null;

    try {
      return md.digest(iInput.getBytes("UTF-8"));
    } catch (UnsupportedEncodingException e) {
      final String message = "The requested encoding is not supported: cannot execute security checks";
      LogManager.instance().error(this, message, e);

      throw new ConfigurationException(message, e);
    }
  }

  /**
   * Returns true if the algorithm is supported by the current version of Java
   */
  private static boolean isAlgorithmSupported(final String algorithm) {
    // Java 7 specific checks.
    if (Runtime.class.getPackage() != null && Runtime.class.getPackage().getImplementationVersion() != null) {
      if (Runtime.class.getPackage().getImplementationVersion().startsWith("1.7")) {
        // Java 7 does not support the PBKDF2_SHA256_ALGORITHM.
        if (algorithm != null && algorithm.equals(PBKDF2_SHA256_ALGORITHM)) {
          return false;
        }
      }
    }

    return true;
  }

  private String validateAlgorithm(final String iAlgorithm) {
    String validAlgo = iAlgorithm;

    if (!isAlgorithmSupported(iAlgorithm)) {
      // Downgrade it to PBKDF2_ALGORITHM.
      validAlgo = PBKDF2_ALGORITHM;

      LogManager.instance().debug(this, "The %s algorithm is not supported, downgrading to %s", iAlgorithm, validAlgo);
    }

    return validAlgo;
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

  private static byte[] hexToByteArray(final String data) {
    final byte[] hex = new byte[data.length() / 2];
    for (int i = 0; i < hex.length; i++)
      hex[i] = (byte) Integer.parseInt(data.substring(2 * i, 2 * i + 2), 16);

    return hex;
  }
}
