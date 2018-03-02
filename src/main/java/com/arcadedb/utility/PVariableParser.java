package com.arcadedb.utility;

/**
 * Resolve entity class and descriptors using the paths configured.
 *
 * @author Luca Garulli (luca.garulli--at--assetdata.it)
 */
public class PVariableParser {
  public static Object resolveVariables(final String iText, final String iBegin, final String iEnd,
      final PVariableParserListener iListener) {
    return resolveVariables(iText, iBegin, iEnd, iListener, null);
  }

  public static Object resolveVariables(final String iText, final String iBegin, final String iEnd,
      final PVariableParserListener iListener, final Object iDefaultValue) {
    if (iListener == null)
      throw new IllegalArgumentException("Missed VariableParserListener listener");

    int beginPos = iText.lastIndexOf(iBegin);
    if (beginPos == -1)
      return iText;

    int endPos = iText.indexOf(iEnd, beginPos + 1);
    if (endPos == -1)
      return iText;

    String pre = iText.substring(0, beginPos);
    String var = iText.substring(beginPos + iBegin.length(), endPos);
    String post = iText.substring(endPos + iEnd.length());

    Object resolved = iListener.resolve(var);

    if (resolved == null) {
      if (iDefaultValue == null)
        PLogManager.instance().info(null, "[OVariableParser.resolveVariables] Error on resolving property: %s", var);
      else
        resolved = iDefaultValue;
    }

    if (pre.length() > 0 || post.length() > 0) {
      final String path = pre + (resolved != null ? resolved.toString() : "") + post;
      return resolveVariables(path, iBegin, iEnd, iListener);
    }

    return resolved;
  }
}
