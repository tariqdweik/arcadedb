/*
 * Copyright (c) 2018 - Arcade Analytics LTD (https://arcadeanalytics.com)
 */
package com.arcadedb.sql.method.misc;

import com.arcadedb.database.Identifiable;
import com.arcadedb.sql.executor.CommandContext;
import com.arcadedb.utility.PatternConst;
import com.arcadedb.utility.FileUtils;

import java.text.Normalizer;

/**
 * @author Johann Sorel (Geomatys)
 * @author Luca Garulli (l.garulli--(at)--orientdb.com)
 */
public class SQLMethodNormalize extends OAbstractSQLMethod {

  public static final String NAME = "normalize";

  public SQLMethodNormalize() {
    super(NAME, 0, 2);
  }

  @Override
  public Object execute( final Object iThis, Identifiable iCurrentRecord, CommandContext iContext,
      Object ioResult, Object[] iParams) {

    if (ioResult != null) {
      final Normalizer.Form form = iParams != null && iParams.length > 0 ?
          Normalizer.Form.valueOf(FileUtils.getStringContent(iParams[0].toString())) :
          Normalizer.Form.NFD;

      String normalized = Normalizer.normalize(ioResult.toString(), form);
      if (iParams != null && iParams.length > 1) {
        normalized = normalized.replaceAll(FileUtils.getStringContent(iParams[0].toString()), "");
      } else {
        normalized = PatternConst.PATTERN_DIACRITICAL_MARKS.matcher(normalized).replaceAll("");
      }
      ioResult = normalized;
    }
    return ioResult;
  }
}
